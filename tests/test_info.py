import json

import httpx
import pytest

from hltoolkit.info import WEIGHTS, InfoClient, InfoUnavailable, WeightLimiter


class FakeClock:
    def __init__(self):
        self.t = 1000.0
        self.sleeps = []

    def now(self):
        return self.t

    async def sleep(self, s):
        self.sleeps.append(s)
        self.t += s


def raw_fill(tid, time, coin="ETH", side="B", sz="1.0", px="100.5", sp="0.0", fee="0.01"):
    return {
        "coin": coin,
        "px": px,
        "sz": sz,
        "side": side,
        "time": time,
        "startPosition": sp,
        "dir": "Open Long",
        "closedPnl": "0.0",
        "hash": "0x",
        "oid": 1,
        "crossed": True,
        "fee": fee,
        "tid": tid,
        "feeToken": "USDC",
    }


def make(handler, per_minute=100000):
    clock = FakeClock()
    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return InfoClient("http://x/info", client, WeightLimiter(per_minute, clock), clock), clock


async def test_fills_by_time_paginates():
    page1 = [raw_fill(i, 1000 + i) for i in range(2000)]
    page2 = [raw_fill(1999, 2999)] + [raw_fill(5000 + i, 3000 + i) for i in range(5)]  # tid 1999 duplicate
    bodies = []

    def handler(req):
        b = json.loads(req.content)
        bodies.append(b)
        return httpx.Response(200, json=page1 if len(bodies) == 1 else page2)

    info, _ = make(handler)
    fills = await info.fills_by_time("0xabc", 5)
    assert len(fills) == 2000 + 5
    assert bodies[0] == {"type": "userFillsByTime", "user": "0xabc", "startTime": 5}
    assert bodies[1]["startTime"] == 1000 + 1999  # from the last page's final millisecond (its tids are skipped)
    f = fills[0]
    assert (f.user, f.coin, f.px, f.sz, f.side, f.time_ms, f.start_position, f.tid, f.fee) == (
        "0xabc",
        "ETH",
        100.5,
        1.0,
        "B",
        1000,
        0.0,
        0,
        0.01,
    )


async def test_fills_by_time_stops_at_50_pages_and_keeps_end():
    n = 0

    def handler(req):
        nonlocal n
        n += 1
        return httpx.Response(200, json=[raw_fill(n * 10000 + i, n * 10000 + i) for i in range(2000)])

    info, _ = make(handler)
    await info.fills_by_time("0xabc", 0, end_ms=10**9)
    assert n == 50


async def test_retries_then_unavailable():
    calls = 0

    def handler(req):
        nonlocal calls
        calls += 1
        return httpx.Response(429)

    info, clock = make(handler)
    with pytest.raises(InfoUnavailable):
        await info.l2_book("ETH")
    assert calls == 4
    assert [s for s in clock.sleeps if s in (0.5, 1.0, 2.0)] == [0.5, 1.0, 2.0]


async def test_retries_transport_error_then_succeeds():
    calls = 0

    def handler(req):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError("boom")
        if calls == 2:
            return httpx.Response(503)
        return httpx.Response(200, json={"coin": "ETH", "levels": [[], []]})

    info, _ = make(handler)
    assert (await info.l2_book("ETH"))["coin"] == "ETH"
    assert calls == 3


async def test_other_status_fails_immediately():
    calls = 0

    def handler(req):
        nonlocal calls
        calls += 1
        return httpx.Response(422, text="bad")

    info, _ = make(handler)
    with pytest.raises(InfoUnavailable):
        await info.l2_book("ETH")
    assert calls == 1


@pytest.mark.parametrize("body", ["not json", '{"levels": 3}', "[]", '[{"a":1}]'])
async def test_malformed_body_unavailable(body):
    info, _ = make(lambda req: httpx.Response(200, text=body))
    with pytest.raises(InfoUnavailable):
        await info.l2_book("ETH")


async def test_malformed_fills_unavailable():
    info, _ = make(lambda req: httpx.Response(200, json=[{"coin": "ETH"}]))
    with pytest.raises(InfoUnavailable):
        await info.fills_by_time("0xabc", 0)


async def test_meta_and_ctxs_and_clearinghouse_and_funding():
    def handler(req):
        t = json.loads(req.content)["type"]
        if t == "metaAndAssetCtxs":
            return httpx.Response(200, json=[{"universe": [{"name": "BTC"}]}, [{"markPx": "1"}]])
        if t == "clearinghouseState":
            return httpx.Response(200, json={"marginSummary": {}, "assetPositions": []})
        return httpx.Response(200, json=[{"coin": "ETH", "fundingRate": "0.0001", "premium": "0", "time": 5}])

    info, _ = make(handler)
    uni, ctxs = await info.meta_and_ctxs()
    assert uni == [{"name": "BTC"}] and ctxs == [{"markPx": "1"}]
    assert "assetPositions" in await info.clearinghouse_state("0xabc")
    assert (await info.funding_history("ETH", 0, 10))[0]["fundingRate"] == "0.0001"


async def test_portfolio_parsed():
    body = [
        ["day", {"accountValueHistory": [[1, "10.5"], [2, "11"]], "pnlHistory": [[1, "0"], [2, "0.5"]], "vlm": "1"}],
        ["perpDay", {"accountValueHistory": [], "pnlHistory": [], "vlm": "0"}],
    ]
    info, _ = make(lambda req: httpx.Response(200, json=body))
    p = await info.portfolio("0xabc")
    assert p["day"] == {"accountValueHistory": [(1, 10.5), (2, 11.0)], "pnlHistory": [(1, 0.0), (2, 0.5)]}
    assert p["perpDay"]["pnlHistory"] == []


def test_weights_table():
    assert WEIGHTS["l2Book"] == 2 and WEIGHTS["clearinghouseState"] == 2 and WEIGHTS["allMids"] == 2
    for k in ("metaAndAssetCtxs", "fundingHistory", "portfolio", "userFillsByTime"):
        assert WEIGHTS[k] == 20


async def test_limiter_blocks_past_weight():
    clock = FakeClock()
    lim = WeightLimiter(10, clock)
    await lim.acquire(6)
    assert clock.sleeps == []
    await lim.acquire(4)
    assert clock.sleeps == []
    await lim.acquire(5)  # needs the first 6 to age out
    assert clock.sleeps and clock.sleeps[-1] == pytest.approx(60.0)


async def test_limiter_concurrent_reserves_before_sleeping():
    import asyncio

    class YieldingClock(FakeClock):
        async def sleep(self, s):
            await asyncio.sleep(0)  # let other tasks run while "sleeping"
            self.t += s

    clock = YieldingClock()
    lim = WeightLimiter(10, clock)
    start = clock.t
    done = []

    async def one():
        await lim.acquire(6)
        done.append(clock.t)

    await asyncio.gather(*(one() for _ in range(4)))
    # every acquire's return time is its reserved slot or later; no 60 s window holds more than 10
    times = sorted(done)
    for t in times:
        assert sum(6 for u in times if t - 60 < u <= t) <= 10
    assert times[-1] - start >= 180 - 1e-6
    # the reserved slots themselves must respect the cap (this is what catches reserve-after-sleep)
    slots = sorted(t for t, _ in lim._entries)
    assert len(slots) == 4
    for t in slots:
        assert sum(6 for u in slots if t - 60 < u <= t) <= 10


async def test_fills_full_page_same_timestamp_terminates_and_warns(caplog):
    n = 0

    def handler(req):
        nonlocal n
        n += 1
        return httpx.Response(200, json=[raw_fill(i, 5000) for i in range(2000)])  # every page: one millisecond

    info, _ = make(handler)
    with caplog.at_level("WARNING", logger="hltoolkit.info"):
        fills = await info.fills_by_time("0xabc", 0)
    assert len(fills) == 2000 and n == 2  # page 2 starts at 5000, gets nowhere further: stop
    assert "same-ms" in caplog.text


async def test_fills_sharing_the_last_millisecond_of_a_page_are_not_lost():
    late = [raw_fill(10_000 + i, 5000) for i in range(5)]  # five fills at 5000: three fit on page 1
    page1 = [raw_fill(i, 1000 + i) for i in range(1997)] + late[:3]
    bodies = []

    def handler(req):
        b = json.loads(req.content)
        bodies.append(b)
        return httpx.Response(200, json=page1 if len(bodies) == 1 else late + [raw_fill(20_000, 6000)])

    info, _ = make(handler)
    fills = await info.fills_by_time("0xabc", 0)
    assert bodies[1]["startTime"] == 5000  # the last page's final millisecond again
    assert len(fills) == 1997 + 5 + 1 and len({f.tid for f in fills}) == len(fills)


async def test_fills_non_advancing_page_stops():
    n = 0

    def handler(req):
        nonlocal n
        n += 1
        return httpx.Response(200, json=[raw_fill(i, 100) for i in range(2000)])

    info, _ = make(handler)
    await info.fills_by_time("0xabc", 5000)
    assert n == 1


async def test_all_mids_parsed_and_weighs_two():
    bodies = []

    def handler(req):
        bodies.append(json.loads(req.content))
        return httpx.Response(200, json={"BTC": "65000.5", "ETH": "3200", "@107": "1.2", "BAD": "x"})

    info, _ = make(handler)
    charged = []
    real = info._limiter.acquire

    async def spy(w):
        charged.append(w)
        await real(w)

    info._limiter.acquire = spy
    mids = await info.all_mids()
    assert bodies == [{"type": "allMids"}] and charged == [2]
    assert mids == {"BTC": 65000.5, "ETH": 3200.0, "@107": 1.2}  # unparseable values dropped


@pytest.mark.parametrize("body", ["[]", "{}", '{"BTC": "x"}', "null"])
async def test_all_mids_malformed_unavailable(body):
    info, _ = make(lambda req: httpx.Response(200, text=body))
    with pytest.raises(InfoUnavailable):
        await info.all_mids()


# ----------------------------------------------------------------------------- candles and trades


def spy_weights(info):
    charged = []
    real = info._limiter.acquire

    async def spy(w):
        charged.append(w)
        await real(w)

    info._limiter.acquire = spy
    return charged


def raw_candle(t, o="100", c="101", h="102", lo="99", v="5.5"):
    return {"t": t, "T": t + 899_999, "s": "ETH", "i": "15m", "o": o, "c": c, "h": h, "l": lo, "v": v, "n": 7}


def test_trade_view_weights_follow_the_docs():
    # rate-limits page: neither is in the weight-2 list, so both are the default 20;
    # recentTrades adds 1 per 20 items returned, candleSnapshot 1 per 60
    assert WEIGHTS["candleSnapshot"] == 20 and WEIGHTS["recentTrades"] == 20


async def test_candles_request_parse_and_weight():
    bodies = []

    def handler(req):
        bodies.append(json.loads(req.content))
        return httpx.Response(200, json=[raw_candle(1_000_000 + 900_000 * i) for i in range(301)])

    info, _ = make(handler)
    charged = spy_weights(info)
    candles = await info.candles("ETH", "15m", 1_000, 2_000)
    assert bodies == [
        {"type": "candleSnapshot", "req": {"coin": "ETH", "interval": "15m", "startTime": 1_000, "endTime": 2_000}}
    ]
    assert charged == [20, 5]  # base, then 301 // 60 for the items returned
    assert len(candles) == 301
    assert candles[0] == {"time": 1_000_000, "open": 100.0, "high": 102.0, "low": 99.0, "close": 101.0, "volume": 5.5}


async def test_candles_few_items_no_extra_weight():
    info, _ = make(lambda req: httpx.Response(200, json=[raw_candle(60_000)]))
    charged = spy_weights(info)
    assert len(await info.candles("ETH", "1m", 0, 1)) == 1
    assert charged == [20]


BAD_CANDLES = ["{}", "null", '[{"t": 1}]', '[{"t": 1, "o": "x", "c": "1", "h": "1", "l": "1", "v": "1"}]']


@pytest.mark.parametrize("body", BAD_CANDLES)
async def test_candles_malformed_unavailable(body):
    info, _ = make(lambda req: httpx.Response(200, text=body))
    with pytest.raises(InfoUnavailable):
        await info.candles("ETH", "1h", 0, 1)


async def test_recent_trades_parse_drop_users_and_weight():
    bodies = []
    raw = [
        {
            "coin": "ETH",
            "side": "A" if i % 2 else "B",
            "px": "2052.7",
            "sz": "0.01",
            "time": 1_794_364_146_025 - i,
            "hash": "0x0",
            "tid": 900 + i,
            "users": ["0xaaa", "0xbbb"],
        }
        for i in range(25)
    ]

    def handler(req):
        bodies.append(json.loads(req.content))
        return httpx.Response(200, json=raw)

    info, _ = make(handler)
    charged = spy_weights(info)
    trades = await info.recent_trades("ETH")
    assert bodies == [{"type": "recentTrades", "coin": "ETH"}]
    assert charged == [20, 1]  # base, then 25 // 20
    assert trades[0] == {"time": 1_794_364_146_025, "px": 2052.7, "sz": 0.01, "side": "buy", "tid": 900}
    assert trades[1]["side"] == "sell"
    assert all("users" not in t for t in trades)


@pytest.mark.parametrize("body", ["{}", "null", '[{"px": "1"}]', '[{"px": "1", "sz": "1", "time": 1, "side": "X"}]'])
async def test_recent_trades_malformed_unavailable(body):
    info, _ = make(lambda req: httpx.Response(200, text=body))
    with pytest.raises(InfoUnavailable):
        await info.recent_trades("ETH")


async def test_limiter_room_and_public_handle():
    clock = FakeClock()
    lim = WeightLimiter(100, clock)
    assert lim.per_minute == 100 and lim.room() == 100
    await lim.acquire(30)
    assert lim.room() == 70
    clock.t += 61  # aged out of the window
    assert lim.room() == 100
    info = InfoClient(
        "http://x/info", httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200))), lim, clock
    )
    assert info.limiter is lim
    await info._client.aclose()


async def test_single_attempt_view_shares_the_budget_and_skips_retries():
    calls = []

    def handler(req):
        calls.append(req.extensions.get("timeout"))
        return httpx.Response(502)

    info, clock = make(handler)
    once = info.single_attempt(5.0)
    assert once.limiter is info.limiter and once.attempts == 1 and once.timeout == 5.0
    assert info.attempts == 4 and info.timeout is None  # the original client is unchanged
    with pytest.raises(InfoUnavailable):
        await once.l2_book("ETH")
    assert len(calls) == 1 and calls[0]["read"] == 5.0 and clock.sleeps == []


# ----------------------------------------------------------------------------- HIP-3 dexes


async def test_dex_reads_add_the_dex_to_the_body_and_keep_their_weight():
    bodies = []

    def handler(req):
        b = json.loads(req.content)
        bodies.append(b)
        if b["type"] == "allMids":
            return httpx.Response(200, json={"xyz:GOLD": "3887.8"})
        if b["type"] == "metaAndAssetCtxs":
            return httpx.Response(
                200, json=[{"universe": [{"name": "xyz:GOLD", "deployerFeeScale": "1.0"}]}, [{"markPx": "3887.8"}]]
            )
        return httpx.Response(200, json={"assetPositions": []})

    info, _ = make(handler)
    charged = []
    real = info._limiter.acquire

    async def spy(w):
        charged.append(w)
        await real(w)

    info._limiter.acquire = spy
    assert await info.all_mids("xyz") == {"xyz:GOLD": 3887.8}
    uni, _ = await info.meta_and_ctxs(dex="xyz")
    assert uni[0]["name"] == "xyz:GOLD"
    await info.clearinghouse_state("0xabc", dex="xyz")
    await info.all_mids()  # the main dex: no "dex" key at all, as before
    assert bodies == [
        {"type": "allMids", "dex": "xyz"},
        {"type": "metaAndAssetCtxs", "dex": "xyz"},
        {"type": "clearinghouseState", "user": "0xabc", "dex": "xyz"},
        {"type": "allMids"},
    ]
    assert charged == [2, 20, 2, 2]


async def test_keep_free_waits_for_room_before_each_page():
    info, clock = make(lambda r: httpx.Response(200, json=[]), per_minute=900)
    await info.limiter.acquire(700)  # other calls used most of this minute
    await info.fills_by_time("0xabc", 0, keep_free=300)
    assert sum(clock.sleeps) >= 60  # waited for the minute to roll over: 120 + 300 must stay free
    info2, clock2 = make(lambda r: httpx.Response(200, json=[]), per_minute=900)
    await info2.fills_by_time("0xabc", 0, keep_free=300)
    assert clock2.sleeps == []  # room enough: no wait


async def test_prediction_market_settlements_are_skipped_not_fatal():
    # HIP-4 outcome coins ("#N") settle at a price of 0: left out, and never a reason to drop the page
    page = [
        raw_fill(1, 1000, coin="BTC"),
        raw_fill(2, 1001, coin="#165420", px="0.0", side="A", sp="70.0"),
        raw_fill(3, 1002, coin="ETH"),
    ]
    info, _ = make(lambda r: httpx.Response(200, json=page))
    fills = await info.fills_by_time("0xabc", 0)
    assert [f.coin for f in fills] == ["BTC", "ETH"]


async def test_other_malformed_fills_still_fail_the_read():
    page = [raw_fill(1, 1000, coin="BTC", px="0.0")]
    info, _ = make(lambda r: httpx.Response(200, json=page))
    with pytest.raises(InfoUnavailable):
        await info.fills_by_time("0xabc", 0)


async def test_perp_dexs_and_meta_bodies_weights_and_validation():
    bodies = []

    def handler(req):
        body = json.loads(req.content)
        bodies.append(body)
        if body["type"] == "perpDexs":
            return httpx.Response(200, json=[None, {"name": "xyz"}])
        return httpx.Response(200, json={"universe": [{"name": "xyz:GOLD", "szDecimals": 4}]})

    info, _ = make(handler)
    charged = []
    real = info._limiter.acquire

    async def spy(w):
        charged.append(w)
        await real(w)

    info._limiter.acquire = spy
    assert await info.perp_dexs() == [None, {"name": "xyz"}]
    assert (await info.meta("xyz"))["universe"][0]["name"] == "xyz:GOLD"
    await info.meta()
    assert bodies == [{"type": "perpDexs"}, {"type": "meta", "dex": "xyz"}, {"type": "meta"}]
    assert charged == [20, 20, 20]


async def test_malformed_perp_dexs_and_meta_are_unavailable():
    info, _ = make(lambda req: httpx.Response(200, json={"oops": 1}))
    with pytest.raises(InfoUnavailable):
        await info.perp_dexs()
    with pytest.raises(InfoUnavailable):
        await info.meta()


async def test_account_readers_send_the_right_bodies_and_weights():
    bodies = []
    answers = {
        "spotClearinghouseState": {"balances": []},
        "orderStatus": {"status": "unknownOid"},
        "frontendOpenOrders": [],
        "userAbstraction": "disabled",
        "userFunding": [],
        "userRole": {"role": "missing"},
    }

    def handler(req):
        b = json.loads(req.content)
        bodies.append(b)
        return httpx.Response(200, json=answers[b["type"]])

    info, clock = make(handler, per_minute=100000)
    weights = []
    real = info._limiter.acquire

    async def spy(w):
        weights.append(w)
        await real(w)

    info._limiter.acquire = spy
    await info.spot_clearinghouse_state("0xa")
    await info.order_status("0xa", "0xc10")
    await info.frontend_open_orders("0xa")
    await info.frontend_open_orders("0xa", "xyz")
    await info.user_abstraction("0xa")
    await info.user_funding("0xa", 7)
    await info.user_role("0xa")
    assert weights == [2, 2, 20, 20, 20, 20, 60]
    assert bodies[1] == {"type": "orderStatus", "user": "0xa", "oid": "0xc10"}
    assert bodies[2] == {"type": "frontendOpenOrders", "user": "0xa"}
    assert bodies[3] == {"type": "frontendOpenOrders", "user": "0xa", "dex": "xyz"}
    assert bodies[5] == {"type": "userFunding", "user": "0xa", "startTime": 7}


async def test_account_readers_refuse_malformed_answers():
    for _kind, bad, call in [
        ("spotClearinghouseState", [], lambda i: i.spot_clearinghouse_state("0xa")),
        ("orderStatus", {"status": "order"}, lambda i: i.order_status("0xa", 1)),
        ("frontendOpenOrders", {}, lambda i: i.frontend_open_orders("0xa")),
        ("userAbstraction", {"x": 1}, lambda i: i.user_abstraction("0xa")),
        ("userFunding", {}, lambda i: i.user_funding("0xa", 0)),
        ("userRole", "agent", lambda i: i.user_role("0xa")),
    ]:
        info, _ = make(lambda req, bad=bad: httpx.Response(200, json=bad))
        with pytest.raises(InfoUnavailable):
            await call(info)


async def test_user_funding_charges_per_20_items():
    items = [{"delta": {"coin": "BTC"}, "hash": "0x", "time": i} for i in range(45)]
    info, _ = make(lambda req: httpx.Response(200, json=items))
    weights = []
    real = info._limiter.acquire

    async def spy(w):
        weights.append(w)
        await real(w)

    info._limiter.acquire = spy
    await info.user_funding("0xa", 0)
    assert weights == [20, 2]


async def test_user_fills_raw_pages_and_keeps_raw_dicts():
    page1 = [raw_fill(i, 1000 + i) for i in range(2000)]
    page2 = [raw_fill(1999, 2999)] + [raw_fill(5000 + i, 3000 + i) for i in range(5)]
    bodies = []

    def handler(req):
        bodies.append(json.loads(req.content))
        return httpx.Response(200, json=page1 if len(bodies) == 1 else page2)

    info, _ = make(handler)
    fills = await info.user_fills_raw("0xabc", 5)
    assert len(fills) == 2005 and fills[0] == page1[0] and "liquidation" not in fills[0]
    assert bodies[1]["startTime"] == 2999


async def test_user_fills_raw_refuses_a_fill_without_tid():
    bad = raw_fill(1, 10)
    del bad["tid"]
    info, _ = make(lambda req: httpx.Response(200, json=[bad]))
    with pytest.raises(InfoUnavailable):
        await info.user_fills_raw("0xabc", 0)


def raw_funding(t, coin="BTC"):
    return {
        "delta": {
            "type": "funding",
            "coin": coin,
            "usdc": "-0.1",
            "szi": "1",
            "fundingRate": "0.0001",
            "nSamples": None,
        },
        "hash": "0x0",
        "time": t,
    }


async def test_user_funding_pages_past_a_500_row_cap_and_drops_the_boundary_duplicate():
    p1 = [raw_funding(1000 + i) for i in range(500)]
    p2 = [raw_funding(1499)] + [raw_funding(2000 + i) for i in range(3)]  # first row repeats the boundary
    bodies = []

    def handler(req):
        bodies.append(json.loads(req.content))
        return httpx.Response(200, json=p1 if len(bodies) == 1 else p2 if len(bodies) == 2 else [p2[-1]])

    info, _ = make(handler)
    rows = await info.user_funding("0xa", 5)
    assert len(rows) == 503 and bodies[1]["startTime"] == 1499
    assert len(bodies) == 2  # the 4-row page is under 500: the last, no confirming request


async def test_a_499_row_page_is_final_and_a_500_row_page_asks_for_the_next():
    for n, calls in ((499, 1), (500, 2)):
        bodies = []

        def handler(req, n=n, bodies=bodies):
            bodies.append(1)
            return httpx.Response(200, json=[raw_funding(1000 + i) for i in range(n)])

        info, _ = make(handler)
        await info.user_funding("0xa", 0)
        assert len(bodies) == calls


async def test_user_funding_that_cannot_advance_raises():
    page = [raw_funding(1000) for _ in range(500)]  # a full page at one instant, same coin: no way forward
    info, _ = make(lambda req: httpx.Response(200, json=[{**r, "hash": f"0x{i}"} for i, r in enumerate(page)]))
    with pytest.raises(InfoUnavailable):
        await info.user_funding("0xa", 1000)


async def test_user_fills_raw_pages_past_a_500_row_cap():
    p1 = [raw_fill(i, 1000 + i) for i in range(500)]
    p2 = [raw_fill(499, 1499), raw_fill(9000, 1600)]
    bodies = []

    def handler(req):
        bodies.append(json.loads(req.content))
        return httpx.Response(200, json=p1 if len(bodies) == 1 else p2)

    info, _ = make(handler)
    fills = await info.user_fills_raw("0xa", 5)
    assert len(fills) == 501 and bodies[1]["startTime"] == 1499


async def test_user_fills_raw_that_cannot_advance_raises():
    info, _ = make(lambda req: httpx.Response(200, json=[raw_fill(i, 1000) for i in range(2000)]))
    with pytest.raises(InfoUnavailable):
        await info.user_fills_raw("0xa", 1000)


async def test_user_fills_raw_hitting_the_page_cap_raises():
    n = 0

    def handler(req):
        nonlocal n
        n += 1
        return httpx.Response(200, json=[raw_fill(n * 10 + i, n * 10 + i) for i in range(2000)])

    info, _ = make(handler)
    with pytest.raises(InfoUnavailable):
        await info.user_fills_raw("0xa", 0)


async def test_fills_by_time_names_fills_by_label_and_keeps_the_raw_fill():
    info, _ = make(lambda r: httpx.Response(200, json=[raw_fill(1, 1000)]))
    [f] = await info.fills_by_time("0xabc", 0, label="alice")
    assert f.user == "alice" and f.raw["dir"] == "Open Long"


async def test_defaults_make_and_close_their_own_client():
    from hltoolkit.info import IP_WEIGHT_PER_MINUTE, MAINNET_INFO_URL

    async with InfoClient() as info:
        assert info._url == MAINNET_INFO_URL and info.limiter.per_minute == IP_WEIGHT_PER_MINUTE == 1200
        once = info.single_attempt(1.0)
        await once.aclose()  # a view never closes the client it shares
        assert not info._client.is_closed
    assert info._client.is_closed


async def test_a_client_passed_in_is_left_open():
    client = httpx.AsyncClient(transport=httpx.MockTransport(lambda r: httpx.Response(200, json={})))
    async with InfoClient("http://x/info", client):
        pass
    assert not client.is_closed
    await client.aclose()
