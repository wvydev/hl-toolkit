import asyncio
import json
import logging

import pytest
import websockets

from hltoolkit.ws import MAX_WS_USERS, FillsFeed

A1 = "0x" + "a1" * 20
A2 = "0x" + "a2" * 20
A3 = "0x" + "a3" * 20


class FastClock:
    def __init__(self):
        self.sleeps = []

    def now(self):
        return 5.0

    async def sleep(self, s):
        self.sleeps.append(s)
        await asyncio.sleep(0)


def raw(tid, user_coin="ETH"):
    return {
        "coin": user_coin,
        "px": "10.5",
        "sz": "2",
        "side": "A",
        "time": 1700000000000 + tid,
        "startPosition": "3",
        "dir": "x",
        "closedPnl": "0",
        "hash": "h",
        "oid": 1,
        "crossed": True,
        "fee": "0.02",
        "tid": tid,
        "feeToken": "USDC",
    }


def userfills(user, tid):
    return json.dumps({"channel": "userFills", "data": {"user": user, "fills": [raw(tid)]}})


async def serve(handler):
    server = await websockets.serve(handler, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    return server, f"ws://127.0.0.1:{port}"


async def test_ws_parses_fills_and_skips_snapshot():
    subs = []

    async def handler(ws):
        async for msg in ws:
            m = json.loads(msg)
            if m["method"] == "subscribe":
                subs.append(m["subscription"])
                if len(subs) == 2:
                    await ws.send(json.dumps({"channel": "subscriptionResponse", "data": m}))
                    await ws.send(
                        json.dumps(
                            {
                                "channel": "userFills",
                                "data": {"isSnapshot": True, "user": A1.lower(), "fills": [raw(1)]},
                            }
                        )
                    )
                    await ws.send(json.dumps({"channel": "pong"}))
                    await ws.send(
                        json.dumps(
                            {
                                "channel": "userFills",
                                "data": {"user": A1.lower(), "fills": [raw(2), raw(3, "xyz:GOLD")]},
                            }
                        )
                    )
                    await ws.send(
                        json.dumps(
                            {
                                "channel": "userFills",
                                "data": {"isSnapshot": False, "user": "0xunknown", "fills": [raw(4)]},
                            }
                        )
                    )
                    await ws.send(json.dumps({"channel": "userFills", "data": {"user": A2, "fills": [raw(5)]}}))

    server, url = await serve(handler)
    feed = FillsFeed({"T1": A1, "T2": A2}, url, clock=FastClock())
    got = []

    async def consume():
        async for f in feed.fills():
            got.append(f)
            if len(got) == 3:
                return

    try:
        await asyncio.wait_for(consume(), 5)
    finally:
        server.close()
    assert sorted(s["user"] for s in subs) == sorted([A1, A2])
    assert all(s["type"] == "userFills" for s in subs)
    assert [(f.user, f.tid, f.coin) for f in got] == [("T1", 2, "ETH"), ("T1", 3, "xyz:GOLD"), ("T2", 5, "ETH")]
    f = got[0]
    assert (f.px, f.sz, f.side, f.start_position, f.fee, f.time_ms) == (10.5, 2.0, "A", 3.0, 0.02, 1700000000002)
    assert feed.last_message_at == 5.0


async def test_ws_reconnects_after_close():
    conns = 0

    async def handler(ws):
        nonlocal conns
        conns += 1
        n = conns
        await ws.recv()
        if n == 1:
            await ws.close()
            return
        await ws.send(json.dumps({"channel": "userFills", "data": {"user": A1, "fills": [raw(9)]}}))
        await asyncio.sleep(1)

    server, url = await serve(handler)
    clock = FastClock()
    feed = FillsFeed({"T1": A1}, url, clock=clock)
    assert feed.connected is False
    try:

        async def first():
            async for f in feed.fills():
                return f

        f = await asyncio.wait_for(first(), 5)
    finally:
        server.close()
    assert f.tid == 9 and conns == 2
    assert clock.sleeps[0] == 1  # first backoff


async def test_ws_connect_failure_backs_off_and_never_raises():
    clock = FastClock()
    attempts = 0

    def bad_connect(url, **kw):
        nonlocal attempts
        attempts += 1
        raise OSError("refused")

    feed = FillsFeed({"T1": A1}, "ws://nowhere", clock=clock, connect=bad_connect)
    task = asyncio.create_task(feed.fills().__anext__())
    while attempts < 8:
        await asyncio.sleep(0.01)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert clock.sleeps[:7] == [1, 2, 4, 8, 16, 30, 30]
    assert feed.connected is False


async def test_ws_sends_ping():
    pings = asyncio.Event()

    async def handler(ws):
        async for msg in ws:
            if json.loads(msg).get("method") == "ping":
                await ws.send(json.dumps({"channel": "pong"}))
                pings.set()

    server, url = await serve(handler)
    feed = FillsFeed({"T1": A1}, url, clock=FastClock(), ping_interval_s=0.05)
    task = asyncio.create_task(feed.fills().__anext__())
    try:
        await asyncio.wait_for(pings.wait(), 3)
        await asyncio.sleep(0.05)
        assert feed.last_message_at == 5.0
    finally:
        task.cancel()
        server.close()


async def test_ws_ping_send_failure_does_not_leak_and_connect_disables_protocol_ping():
    seen = {}

    class BadWs:
        async def send(self, m):
            if '"ping"' in m:
                raise OSError("dead")

        async def recv(self):
            await asyncio.sleep(0.2)
            raise OSError("closed")

    class Ctx:
        async def __aenter__(self):
            return BadWs()

        async def __aexit__(self, *a):
            return False

    def connect(url, **kw):
        seen.update(kw)
        return Ctx()

    feed = FillsFeed({"T1": A1}, "ws://x", clock=FastClock(), connect=connect, ping_interval_s=0.01)
    task = asyncio.create_task(feed.fills().__anext__())
    await asyncio.sleep(0.1)
    task.cancel()
    assert seen == {"ping_interval": None}


async def test_ws_connected_since_is_the_current_connection():
    seen = []

    async def handler(ws):
        await ws.recv()
        await ws.send(json.dumps({"channel": "userFills", "data": {"user": A1, "fills": [raw(len(seen) + 1)]}}))
        await ws.close()

    class Clock(FastClock):
        t = 100.0

        def now(self):
            return self.t

    server, url = await serve(handler)
    clock = Clock()
    feed = FillsFeed({"T1": A1}, url, clock=clock)
    assert feed.connected_since is None
    try:

        async def two():
            async for _ in feed.fills():
                seen.append((feed.connected, feed.connected_since))
                clock.t += 50  # time passes; the next connection starts later
                if len(seen) == 2:
                    return

        await asyncio.wait_for(two(), 5)
    finally:
        server.close()
    assert seen == [(True, 100.0), (True, 150.0)]


async def test_ws_connected_since_cleared_when_down():
    def bad_connect(url, **kw):
        raise OSError("refused")

    feed = FillsFeed({"T1": A1}, "ws://nowhere", clock=FastClock(), connect=bad_connect)
    task = asyncio.create_task(feed.fills().__anext__())
    await asyncio.sleep(0.05)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert feed.connected is False and feed.connected_since is None


async def test_ws_watchdog_reconnects_when_no_frame_arrives(caplog):
    # a half-dead connection (e.g. after a laptop sleeps) can sit silent forever; with no frame
    # (pongs count) for read_timeout_s, the feed closes it and reconnects
    closed = []

    class SilentWs:
        async def send(self, m):
            pass

        async def recv(self):
            await asyncio.Event().wait()  # never answers

    class Ctx:
        async def __aenter__(self):
            return SilentWs()

        async def __aexit__(self, *a):
            closed.append(1)
            return False

    feed = FillsFeed({"T1": A1}, "ws://x", clock=FastClock(), connect=lambda url, **kw: Ctx(), read_timeout_s=0.05)
    task = asyncio.create_task(feed.fills().__anext__())
    try:
        for _ in range(100):
            await asyncio.sleep(0.02)
            if feed.connects >= 3:
                break
    finally:
        task.cancel()
    assert feed.connects >= 3 and len(closed) >= 2
    assert any("no websocket frame" in r.getMessage() for r in caplog.records)


async def test_ws_frames_keep_the_watchdog_quiet():
    async def handler(ws):
        await ws.recv()
        for _ in range(6):  # a frame every 30 ms, read timeout 100 ms: never trips
            await asyncio.sleep(0.03)
            await ws.send(json.dumps({"channel": "pong"}))
        await ws.send(json.dumps({"channel": "userFills", "data": {"user": A1, "fills": [raw(7)]}}))
        await asyncio.sleep(1)

    server, url = await serve(handler)
    try:
        feed = FillsFeed({"T1": A1}, url, clock=FastClock(), read_timeout_s=0.1)
        f = await asyncio.wait_for(feed.fills().__anext__(), 5)
    finally:
        server.close()
    assert f.tid == 7 and feed.connects == 1 and feed.pongs == 6


# --- changing who is followed ---------------------------------------------------------------


async def test_set_users_subscribes_and_unsubscribes_on_the_live_socket():
    seen = [[], []]  # what the server got on each connection: (method, user)
    conns = 0
    subscribed = asyncio.Event()

    async def handler(ws):
        nonlocal conns
        conns += 1
        n, mine = conns, seen[conns - 1]
        async for msg in ws:
            m = json.loads(msg)
            if m.get("method") not in ("subscribe", "unsubscribe"):
                continue
            mine.append((m["method"], m["subscription"]["user"]))
            if n == 1 and len(mine) == 2:
                subscribed.set()
            elif n == 1 and len(mine) == 4:  # set_users' changes are in
                for user, tid in ((A1, 1), (A3, 2), (A2, 3)):  # A1's fill was already on its way
                    await ws.send(userfills(user, tid))
                await ws.close()
                return
            elif n == 2 and len(mine) == 2:
                await ws.send(userfills(A3, 4))
                await asyncio.sleep(1)

    server, url = await serve(handler)
    feed = FillsFeed({"T1": A1, "T2": A2}, url, clock=FastClock())
    got = []

    async def consume():
        async for f in feed.fills():
            got.append((f.user, f.tid))
            if len(got) == 3:
                return

    task = asyncio.create_task(consume())
    try:
        await asyncio.wait_for(subscribed.wait(), 5)
        await feed.set_users({"T2": A2, "T3": A3})  # T1 no longer watched, T3 watched now
        await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        server.close()
    sub = lambda a: ("subscribe", a)  # noqa: E731
    assert seen[0] == [sub(A1), sub(A2), ("unsubscribe", A1), sub(A3)]
    assert got == [("T3", 2), ("T2", 3), ("T3", 4)]  # the removed user's fill is dropped
    assert sorted(seen[1]) == sorted([sub(A2), sub(A3)])  # a reconnect subscribes the current set


async def test_set_users_before_any_connection_only_changes_the_set():
    sent = []

    class Ws:
        async def send(self, m):
            sent.append(json.loads(m))

        async def recv(self):
            await asyncio.Event().wait()

    class Ctx:
        async def __aenter__(self):
            return Ws()

        async def __aexit__(self, *a):
            return False

    feed = FillsFeed({"T1": A1}, "ws://x", clock=FastClock(), connect=lambda url, **kw: Ctx())
    await feed.set_users({"T2": A2})  # nothing is sent: there is no connection yet
    assert sent == []
    task = asyncio.create_task(feed.fills().__anext__())
    try:
        for _ in range(50):
            await asyncio.sleep(0)
            if feed.connected:
                break
    finally:
        task.cancel()
    assert [m["subscription"]["user"] for m in sent] == [A2]


async def test_feed_follows_at_most_ten_users_in_order_and_set_users_frees_a_slot():
    # Hyperliquid serves user websocket subscriptions for at most 10 unique users per IP
    assert MAX_WS_USERS == 10
    users = {f"T{n}": f"0x{n:040x}" for n in range(1, 13)}
    seen = []
    ten = asyncio.Event()

    async def handler(ws):
        async for msg in ws:
            m = json.loads(msg)
            if m.get("method") not in ("subscribe", "unsubscribe"):
                continue
            seen.append((m["method"], m["subscription"]["user"]))
            if len(seen) == MAX_WS_USERS:
                ten.set()
            elif len(seen) == MAX_WS_USERS + 2:
                for n in (12, 11):  # T12 still doesn't fit: a stray frame of theirs is dropped
                    await ws.send(userfills(users[f"T{n}"], n))
                await asyncio.sleep(1)

    server, url = await serve(handler)
    feed = FillsFeed(users, url, clock=FastClock())
    assert feed.overflow == ["T11", "T12"]
    task = asyncio.create_task(feed.fills().__anext__())
    try:
        await asyncio.wait_for(ten.wait(), 5)
        await asyncio.sleep(0.05)  # anything more the feed sent arrives meanwhile
        assert seen == [("subscribe", users[f"T{n}"]) for n in range(1, 11)]
        a5 = users.pop("T5")  # T5's slot goes to T11, next in order
        await feed.set_users(users)
        f = await asyncio.wait_for(task, 5)
    finally:
        task.cancel()
        server.close()
    assert seen[MAX_WS_USERS:] == [("unsubscribe", a5), ("subscribe", users["T11"])]
    assert (f.user, f.tid) == ("T11", 11)
    assert feed.overflow == ["T12"]


def test_a_plain_list_of_addresses_names_each_by_itself():
    feed = FillsFeed([A1, A2], "wss://unused", clock=FastClock())
    assert feed.users == {A1: A1, A2: A2}
    [f] = feed._handle(userfills(A2.upper().replace("0X", "0x"), 4))  # addresses match in any case
    assert (f.user, f.tid) == (A2, 4)


def test_the_feed_subscribes_no_more_users_than_hyperliquid_serves():
    feed = FillsFeed({f"T{i}": f"0x{i}" for i in range(11)}, "wss://unused", clock=FastClock())
    assert len(feed._wanted()) == MAX_WS_USERS and feed.overflow == ["T10"]


def test_an_error_frame_is_logged_counted_and_kept(caplog):
    class Clock(FastClock):
        def now(self):
            return 123.0

    feed = FillsFeed({"T1": "0xT1"}, "wss://unused", clock=Clock())
    with caplog.at_level(logging.WARNING):
        assert feed._handle(json.dumps({"channel": "error", "data": "Cannot track more than 10 total users."})) == []
    assert feed.errors == 1 and "10 total users" in feed.last_error and feed.last_error_at == 123.0
    assert any("10 total users" in r.getMessage() for r in caplog.records)


def test_an_infinite_whole_number_field_skips_only_that_fill():
    # int(Infinity) raises OverflowError: it must not escape the parser and drop the good fill next to it
    feed = FillsFeed({"T1": "0xT1"}, "wss://unused", clock=FastClock())
    good = {"coin": "BTC", "px": "100", "sz": "1", "side": "B", "time": 1, "startPosition": "0", "tid": 3, "fee": "0"}
    msg = json.dumps(
        {
            "channel": "userFills",
            "data": {"user": "0xT1", "fills": [{**good, "tid": "TID"}, {**good, "time": "TIME"}, good]},
        }
    )
    msg = msg.replace('"TID"', "Infinity").replace('"TIME"', "-Infinity")
    assert [f.tid for f in feed._handle(msg)] == [3]


def test_the_raw_fill_is_kept():
    feed = FillsFeed({"T1": A1}, "wss://unused", clock=FastClock())
    [f] = feed._handle(userfills(A1, 8))
    assert f.raw["oid"] == 1 and f.raw["feeToken"] == "USDC" and f.raw["tid"] == 8
