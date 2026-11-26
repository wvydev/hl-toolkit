"""Read-only Hyperliquid Info client. Only POST /info; nothing signs or sends orders.

Every call is charged to a WeightLimiter before it goes out, using the weights from Hyperliquid's
rate-limits page, so a busy program waits instead of getting HTTP 429s. Retryable failures (network
errors, 429, 5xx) are retried with a short backoff; anything else raises InfoUnavailable at once. Answers
are checked for shape and for NaN/infinity before they are returned.
"""

import logging

import httpx

from hltoolkit.clock import Clock, SystemClock
from hltoolkit.models import Fill, parse_fill
from hltoolkit.numbers import finite, positive, whole

__all__ = [
    "MAINNET_INFO_URL",
    "TESTNET_INFO_URL",
    "WEIGHTS",
    "InfoClient",
    "InfoUnavailable",
    "WeightLimiter",
]

log = logging.getLogger("hltoolkit.info")

MAINNET_INFO_URL = "https://api.hyperliquid.xyz/info"
TESTNET_INFO_URL = "https://api.hyperliquid-testnet.xyz/info"

IP_WEIGHT_PER_MINUTE = 1200  # docs: REST requests share 1200 weight per minute per IP
BACKOFFS = (0.5, 1.0, 2.0)
PAGE_SIZE = 2000
MAX_PAGES = 50
MIN_PAGE_CAP = 500  # a time-range answer is capped at 500 or 2000 rows (docs differ): under 500 is the last page

# Docs (rate-limits page): l2Book, allMids, clearinghouseState, orderStatus and spotClearinghouseState
# weigh 2, userRole 60, everything else 20. userFillsByTime, userFunding, fundingHistory and recentTrades add
# 1 per 20 items returned, candleSnapshot 1 per 60 (charged after the call).
WEIGHTS = {
    "l2Book": 2,
    "clearinghouseState": 2,
    "allMids": 2,
    "spotClearinghouseState": 2,
    "orderStatus": 2,
    "metaAndAssetCtxs": 20,
    "perpDexs": 20,
    "meta": 20,
    "fundingHistory": 20,
    "portfolio": 20,
    "userFillsByTime": 20,
    "candleSnapshot": 20,
    "recentTrades": 20,
    "frontendOpenOrders": 20,
    "userAbstraction": 20,
    "userFunding": 20,
    "userRateLimit": 20,
    "userRole": 60,
}
PER_ITEM = {"userFillsByTime": 20, "recentTrades": 20, "candleSnapshot": 60, "userFunding": 20}  # +1 per N items


class InfoUnavailable(Exception):
    """The answer could not be had: retries ran out, a non-retryable status, or a malformed answer."""


class _Retryable(Exception):
    pass


def _on_dex(body: dict, dex: str) -> dict:
    """The body for a HIP-3 dex ("xyz"); the main dex ("") sends no "dex" key."""
    return {**body, "dex": dex} if dex else body


class WeightLimiter:
    """Sliding 60 s window over request weights. Slots are reserved synchronously (no await between
    computing and recording), so concurrent callers can't share capacity."""

    def __init__(self, per_minute: float = IP_WEIGHT_PER_MINUTE, clock: Clock | None = None):
        self._cap = per_minute
        self._clock = clock or SystemClock()
        self._entries: list[tuple[float, float]] = []

    @property
    def per_minute(self) -> float:
        return self._cap

    def room(self) -> float:
        """Weight still free in the 60 s window ending now (slots reserved by callers that are still
        sleeping count as used). Reserves nothing."""
        now = self._clock.now()
        return self._cap - sum(w for t, w in self._entries if t > now - 60)

    async def acquire(self, weight: float) -> None:
        """Reserve `weight`, sleeping until the window has room for it."""
        weight = min(weight, self._cap)
        now = self._clock.now()
        self._entries = [(t, w) for t, w in self._entries if t > now - 60]
        t = now
        while True:
            live = [(ts, w) for ts, w in self._entries if ts > t - 60]
            if sum(w for _, w in live) + weight <= self._cap:
                break
            t = min(ts for ts, _ in live) + 60
        self._entries.append((t, weight))
        if t > now:
            await self._clock.sleep(t - now)


class InfoClient:
    """Read-only calls to Hyperliquid's POST /info.

    With no `client`, it makes and owns an httpx.AsyncClient: close it with `await info.aclose()` or use
    `async with InfoClient() as info:`. Share one `limiter` between clients that use the same IP."""

    def __init__(self, url: str = MAINNET_INFO_URL, client: httpx.AsyncClient | None = None,
                 limiter: WeightLimiter | None = None, clock: Clock | None = None,
                 backoffs: tuple[float, ...] = BACKOFFS, timeout: float | None = None):
        self._url = url
        self._owns_client = client is None
        self._client = client or httpx.AsyncClient(timeout=10.0)
        self._clock = clock or SystemClock()
        self._limiter = limiter or WeightLimiter(IP_WEIGHT_PER_MINUTE, self._clock)
        self._backoffs = tuple(backoffs)
        self.timeout = timeout  # per request; None keeps the httpx client's own

    async def aclose(self) -> None:
        """Close the httpx client if this InfoClient made it."""
        if self._owns_client:
            await self._client.aclose()

    async def __aenter__(self) -> "InfoClient":
        return self

    async def __aexit__(self, *exc) -> None:
        await self.aclose()

    @property
    def attempts(self) -> int:
        return len(self._backoffs) + 1

    @property
    def limiter(self) -> WeightLimiter:
        """The weight budget every call through this client draws from."""
        return self._limiter

    def single_attempt(self, timeout: float) -> "InfoClient":
        """The same client and weight budget, but one try per call with this timeout: for callers (a UI)
        that would rather show the last answer than wait out the retries."""
        once = InfoClient(self._url, self._client, self._limiter, self._clock, backoffs=(), timeout=timeout)
        once._owns_client = False
        return once

    async def _charge_items(self, kind: str, n: int) -> None:
        """The docs' extra weight for list answers, charged after the call."""
        per = PER_ITEM[kind]
        if n >= per:
            await self._limiter.acquire(n // per)

    async def _attempt(self, body: dict):
        try:
            if self.timeout is None:
                resp = await self._client.post(self._url, json=body)
            else:
                resp = await self._client.post(self._url, json=body, timeout=self.timeout)
        except httpx.RequestError as e:
            raise _Retryable(f"{type(e).__name__}: {e}") from e
        if resp.status_code == 429 or resp.status_code >= 500:
            raise _Retryable(f"HTTP {resp.status_code}")
        if resp.status_code != 200:
            raise InfoUnavailable(f"HTTP {resp.status_code}")
        try:
            return resp.json()
        except ValueError as e:
            raise InfoUnavailable("invalid JSON from info endpoint") from e

    async def _post(self, body: dict, weight: float | None = None):
        kind = body["type"]
        w = WEIGHTS[kind] if weight is None else weight
        last = ""
        for delay in (*self._backoffs, None):
            await self._limiter.acquire(w)
            try:
                return await self._attempt(body)
            except _Retryable as e:
                last = str(e)
                if delay is None:
                    break
                await self._clock.sleep(delay)
        raise InfoUnavailable(f"{kind} failed after retries: {last}")

    # --- markets -------------------------------------------------------------------------------

    async def l2_book(self, coin: str) -> dict:
        """The order book: {"coin", "time", "levels": [bids, asks]}, each level {"px", "sz", "n"}."""
        data = await self._post({"type": "l2Book", "coin": coin})
        levels = data.get("levels") if isinstance(data, dict) else None
        if not isinstance(levels, list) or len(levels) != 2 or not all(isinstance(s, list) for s in levels):
            raise InfoUnavailable(f"malformed l2Book for {coin}")
        return data

    async def all_mids(self, dex: str = "") -> dict[str, float]:
        """{coin: mid price}, every market of the dex (the main one by default) in one weight-2 call.
        Unparseable values are dropped."""
        data = await self._post(_on_dex({"type": "allMids"}, dex))
        if not isinstance(data, dict):
            raise InfoUnavailable("malformed allMids")
        mids = {}
        for coin, px in data.items():
            try:
                mids[str(coin)] = positive(px)
            except (TypeError, ValueError):
                continue
        if not mids:
            raise InfoUnavailable("allMids had no prices")
        return mids

    async def meta_and_ctxs(self, dex: str = "") -> tuple[list[dict], list[dict]]:
        """(universe, asset contexts), same length and order: each market's meta next to its live data."""
        data = await self._post(_on_dex({"type": "metaAndAssetCtxs"}, dex))
        try:
            meta, ctxs = data
            universe = meta["universe"]
        except (TypeError, KeyError, ValueError) as e:
            raise InfoUnavailable("malformed metaAndAssetCtxs") from e
        if not isinstance(universe, list) or not isinstance(ctxs, list) or len(universe) != len(ctxs):
            raise InfoUnavailable("malformed metaAndAssetCtxs")
        return universe, ctxs

    async def perp_dexs(self) -> list:
        """The perp dexes: element 0 is null (the main dex), the rest are objects with a "name". A HIP-3
        dex's asset ids use its position in this list."""
        data = await self._post({"type": "perpDexs"})
        if not isinstance(data, list) or not data:
            raise InfoUnavailable("malformed perpDexs")
        return data

    async def meta(self, dex: str = "") -> dict:
        """One dex's meta: {"universe": [...]}; indices in the universe are the asset ids' low part."""
        data = await self._post(_on_dex({"type": "meta"}, dex))
        if not isinstance(data, dict) or not isinstance(data.get("universe"), list):
            raise InfoUnavailable("malformed meta")
        return data

    async def funding_history(self, coin: str, start_ms: int, end_ms: int | None = None) -> list[dict]:
        """A market's funding rates from start_ms, raw: [{"coin", "fundingRate", "premium", "time"}]."""
        body: dict = {"type": "fundingHistory", "coin": coin, "startTime": start_ms}
        if end_ms is not None:
            body["endTime"] = end_ms
        data = await self._post(body)
        if not isinstance(data, list) or not all(isinstance(r, dict) for r in data):
            raise InfoUnavailable("malformed fundingHistory")
        return data

    async def candles(self, coin: str, interval: str, start_ms: int, end_ms: int) -> list[dict]:
        """OHLCV bars, oldest first: {time (bar open, ms), open, high, low, close, volume}."""
        data = await self._post({"type": "candleSnapshot", "req": {
            "coin": coin, "interval": interval, "startTime": start_ms, "endTime": end_ms}})
        if not isinstance(data, list):
            raise InfoUnavailable(f"malformed candleSnapshot for {coin}")
        await self._charge_items("candleSnapshot", len(data))
        try:
            return [{"time": whole(c["t"]), "open": positive(c["o"]), "high": positive(c["h"]), "low": positive(c["l"]),
                     "close": positive(c["c"]), "volume": finite(c["v"])} for c in data]
        except (TypeError, KeyError, ValueError) as e:
            raise InfoUnavailable(f"malformed candle for {coin}: {e!r}") from e

    async def recent_trades(self, coin: str) -> list[dict]:
        """The market's latest public trades, newest first: {time (ms), px, sz, side, tid}. side is the
        taker's, "buy" or "sell". The answer's `users` (both wallets) is dropped."""
        data = await self._post({"type": "recentTrades", "coin": coin})
        if not isinstance(data, list):
            raise InfoUnavailable(f"malformed recentTrades for {coin}")
        await self._charge_items("recentTrades", len(data))
        out = []
        try:
            for t in data:
                side = {"B": "buy", "A": "sell"}[t["side"]]
                out.append({"time": whole(t["time"]), "px": positive(t["px"]), "sz": positive(t["sz"]),
                            "side": side, "tid": t.get("tid")})
        except (TypeError, KeyError, ValueError) as e:
            raise InfoUnavailable(f"malformed trade for {coin}: {e!r}") from e
        return out

    # --- accounts (any address; Hyperliquid's account data is public) ---------------------------

    async def clearinghouse_state(self, user: str, dex: str = "") -> dict:
        """The user's perp account on one dex: a HIP-3 dex's positions are a separate account there."""
        data = await self._post(_on_dex({"type": "clearinghouseState", "user": user}, dex))
        if not isinstance(data, dict) or not isinstance(data.get("assetPositions"), list):
            raise InfoUnavailable("malformed clearinghouseState")
        return data

    async def spot_clearinghouse_state(self, user: str) -> dict:
        """The user's spot balances: {"balances": [{coin, token, hold, total, entryNtl}]}."""
        data = await self._post({"type": "spotClearinghouseState", "user": user})
        if not isinstance(data, dict) or not isinstance(data.get("balances"), list):
            raise InfoUnavailable("malformed spotClearinghouseState")
        return data

    async def order_status(self, user: str, oid_or_cloid: int | str) -> dict:
        """One order by oid, or by cloid ("0x..."): {"status": "order", "order": {...}} or {"status": "unknownOid"}."""
        data = await self._post({"type": "orderStatus", "user": user, "oid": oid_or_cloid})
        if not isinstance(data, dict) or not isinstance(data.get("status"), str):
            raise InfoUnavailable("malformed orderStatus")
        if data["status"] == "order" and not isinstance(data.get("order"), dict):
            raise InfoUnavailable("malformed orderStatus")
        return data

    async def frontend_open_orders(self, user: str, dex: str = "") -> list[dict]:
        """The user's open orders on one dex, with trigger details (stops, take-profits) and cloids."""
        data = await self._post(_on_dex({"type": "frontendOpenOrders", "user": user}, dex))
        if not isinstance(data, list) or not all(isinstance(o, dict) for o in data):
            raise InfoUnavailable("malformed frontendOpenOrders")
        return data

    async def user_abstraction(self, user: str) -> str:
        """The account mode: "unifiedAccount", "portfolioMargin", "disabled", "default" or "dexAbstraction"."""
        data = await self._post({"type": "userAbstraction", "user": user})
        if not isinstance(data, str) or not data:
            raise InfoUnavailable("malformed userAbstraction")
        return data

    async def user_role(self, user: str) -> dict:
        """What the address is: {"role": "user"|"agent"|"vault"|"subAccount"|"missing", "data"?: {...}}."""
        data = await self._post({"type": "userRole", "user": user})
        if not isinstance(data, dict) or not isinstance(data.get("role"), str):
            raise InfoUnavailable("malformed userRole")
        return data

    async def user_rate_limit(self, user: str) -> dict:
        """The address's action budget: {"cumVlm", "nRequestsUsed", "nRequestsCap", "nRequestsSurplus"}."""
        data = await self._post({"type": "userRateLimit", "user": user})
        if not isinstance(data, dict):
            raise InfoUnavailable("malformed userRateLimit")
        return data

    async def portfolio(self, user: str) -> dict:
        """{window: {"accountValueHistory": [(ms, value)], "pnlHistory": [(ms, pnl)]}} for each window
        Hyperliquid reports ("day", "week", "month", "allTime", and their "perp" versions)."""
        data = await self._post({"type": "portfolio", "user": user})
        try:
            out = {}
            for window, body in data:
                out[window] = {
                    "accountValueHistory": [(whole(t), finite(v)) for t, v in body["accountValueHistory"]],
                    "pnlHistory": [(whole(t), finite(v)) for t, v in body["pnlHistory"]],
                }
        except (TypeError, KeyError, ValueError) as e:
            raise InfoUnavailable("malformed portfolio") from e
        return out

    async def _pages(self, body: dict, kind: str, key_of) -> list[dict]:
        """A time-range read, paged all or nothing. The page size is unknown (500 or 2000), so it pages until
        a page adds nothing new, restarting at the last row's time (rows sharing that instant come again and
        are skipped by key). A page under MIN_PAGE_CAP rows is the last. A page that may be full and cannot
        move forward, or the page cap, raises InfoUnavailable."""
        out: list[dict] = []
        seen: set = set()
        start = body["startTime"]
        for _ in range(MAX_PAGES):
            page = await self._post({**body, "startTime": start})
            if not isinstance(page, list) or not all(isinstance(r, dict) for r in page):
                raise InfoUnavailable(f"malformed {kind}")
            try:
                keys = [key_of(r) for r in page]
                last_ms = max(whole(r["time"]) for r in page) if page else start
            except (TypeError, KeyError, ValueError, OverflowError) as e:
                raise InfoUnavailable(f"malformed row in {kind}: {e!r}") from e
            new = 0
            for r, k in zip(page, keys, strict=True):
                if k not in seen:
                    seen.add(k)
                    out.append(r)
                    new += 1
            if kind in PER_ITEM:
                await self._charge_items(kind, len(page))
            if len(page) < MIN_PAGE_CAP or new == 0:  # under the smallest cap: complete, no confirming request
                return out
            if last_ms <= start:
                raise InfoUnavailable(f"{kind} page full at one millisecond: rows past it cannot be reached")
            start = last_ms
        raise InfoUnavailable(f"{kind} hit the {MAX_PAGES} page cap: the answer would be cut short")

    async def user_funding(self, user: str, start_ms: int) -> list[dict]:
        """The user's funding payments from start_ms, raw: [{"delta": {...}, "hash", "time"}], every page."""
        return await self._pages(
            {"type": "userFunding", "user": user, "startTime": start_ms}, "userFunding",
            lambda r: (whole(r["time"]), str(r["delta"]["coin"]), str(r.get("hash"))))

    async def user_fills_raw(self, user: str, start_ms: int) -> list[dict]:
        """The user's fills from start_ms as raw dicts, oldest first, every page, tids de-duplicated.
        Nothing is parsed or dropped here."""
        return await self._pages(
            {"type": "userFillsByTime", "user": user, "startTime": start_ms}, "userFillsByTime",
            lambda r: whole(r["tid"]))

    async def _leave_free(self, keep_free: float, weight: float) -> None:
        """Wait (in short steps, at most about five minutes) until a request of `weight` would still leave
        `keep_free` of the window unused, so a long background read never crowds out your other calls."""
        for _ in range(60):
            if keep_free <= 0 or self._limiter.room() - weight >= keep_free:
                return
            await self._clock.sleep(5)
        log.warning("the weight budget stayed busy for 5 minutes: a background read goes ahead anyway")

    async def fills_by_time(self, user: str, start_ms: int, end_ms: int | None = None,
                            *, label: str | None = None, keep_free: float = 0.0) -> list[Fill]:
        """The user's fills from start_ms (to end_ms) as checked Fills, oldest first, paging 2,000 at a time up
        to 50 pages. Each Fill's `user` is `label` (the address by default). With keep_free, each page first
        waits until that much of the per-minute weight would stay free.

        Prediction-market outcome fills (HIP-4, coin "#N") settle at a price of 0 and are left out rather than
        failing the read; any other broken fill raises InfoUnavailable."""
        name = user if label is None else label
        out: list[Fill] = []
        seen: set[int] = set()
        start = start_ms
        page_weight = WEIGHTS["userFillsByTime"] + PAGE_SIZE // PER_ITEM["userFillsByTime"]
        for _ in range(MAX_PAGES):
            await self._leave_free(keep_free, page_weight)
            body: dict = {"type": "userFillsByTime", "user": user, "startTime": start}
            if end_ms is not None:
                body["endTime"] = end_ms
            page = await self._post(body)
            if not isinstance(page, list):
                raise InfoUnavailable("malformed userFillsByTime")
            try:
                fills = [parse_fill(name, f) for f in page if not _outcome(f)]
                last_ms = max(whole(f["time"]) for f in page) if page else start
            except (TypeError, KeyError, ValueError, OverflowError) as e:
                raise InfoUnavailable(f"malformed fill in userFillsByTime: {e!r}") from e
            for f in fills:
                if f.tid not in seen:
                    seen.add(f.tid)
                    out.append(f)
            await self._charge_items("userFillsByTime", len(page))
            if len(page) < PAGE_SIZE:
                break
            if last_ms <= start:  # a whole page in one millisecond: nothing more can be reached by time
                log.warning("full page for %s all at %d: same-ms fills past it may be missing", name, last_ms)
                break
            # the next page starts at the last page's final millisecond, not after it: fills that share it and
            # didn't fit are fetched too (the tids seen already are skipped)
            start = last_ms
        else:
            log.warning("fills_by_time for %s hit the %d page cap", name, MAX_PAGES)
        return out


def _outcome(f) -> bool:
    """A prediction-market outcome fill (HIP-4, coin "#N")."""
    return isinstance(f, dict) and str(f.get("coin", "")).startswith("#")
