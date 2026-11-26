"""Websocket feed of accounts' fills (the userFills channel). Read-only subscriptions.

The feed reconnects on its own with a backoff (1 s doubling to 30 s), sends Hyperliquid's app-level
ping, and treats a connection that has sent nothing for READ_TIMEOUT_S as dead even when the socket
still looks open (a half-open TCP connection after a laptop slept or the network dropped). The first
message after each subscribe is a snapshot of recent fills; it is skipped, so only new fills come out.
Fills missed while disconnected are not replayed: read them with InfoClient.fills_by_time.
"""

import asyncio
import json
import logging
from collections.abc import AsyncIterator, Iterable, Mapping

import websockets
from websockets.exceptions import ConnectionClosedOK

from hltoolkit.clock import Clock, SystemClock
from hltoolkit.models import Fill, parse_fill

__all__ = ["MAINNET_WS_URL", "MAX_WS_USERS", "TESTNET_WS_URL", "FillsFeed"]

log = logging.getLogger("hltoolkit.ws")

MAINNET_WS_URL = "wss://api.hyperliquid.xyz/ws"
TESTNET_WS_URL = "wss://api.hyperliquid-testnet.xyz/ws"

# Hyperliquid serves user-specific websocket subscriptions for at most this many unique users per IP
# ("rate limits and user limits" in its docs); more get an error frame, not fills.
MAX_WS_USERS = 10

BACKOFF_START = 1.0
BACKOFF_MAX = 30.0
# Docs: the server closes a connection that has received nothing from it for 60 s; the client sends ping.
PING_INTERVAL_S = 30.0
# No frame at all (pongs count) for this long: the connection is dead even if the socket isn't closed.
# Two and a half ping periods.
READ_TIMEOUT_S = 75.0
ERROR_TEXT_MAX = 300

Users = Mapping[str, str] | Iterable[str]


def _named(users: Users) -> dict[str, str]:
    """name -> address. A plain list of addresses names each one by itself."""
    if isinstance(users, Mapping):
        return dict(users)
    return {addr: addr for addr in users}


def _subscription(method: str, addr: str) -> str:
    return json.dumps({"method": method, "subscription": {"type": "userFills", "user": addr}})


class FillsFeed:
    """New fills of up to MAX_WS_USERS accounts, as checked Fills.

        feed = FillsFeed({"alice": "0x...", "bob": "0x..."})
        async for fill in feed.fills():
            print(fill.user, fill.coin, fill.side, fill.sz, fill.px)

    `users` is {name: address} or a list of addresses (each named by itself). Past MAX_WS_USERS, the
    first ten in order are followed and the rest are listed in `overflow`. `fills()` never raises: a
    broken connection is logged and reconnected. Error frames from Hyperliquid (a refused subscription)
    are counted in `errors` and kept in `last_error`."""

    def __init__(self, users: Users, url: str = MAINNET_WS_URL, *, clock: Clock | None = None, connect=None,
                 ping_interval_s: float = PING_INTERVAL_S, read_timeout_s: float = READ_TIMEOUT_S):
        self._url = url
        self._users: dict[str, str] = {}  # name -> address, the ones followed
        self._by_addr: dict[str, str] = {}  # lowercase address -> name
        self.overflow: list[str] = []  # names given, but beyond MAX_WS_USERS
        self._follow(users)
        self._ws = None  # the live connection, once its subscriptions are sent
        self._live: dict[str, str] = {}  # lowercase address -> address as subscribed on self._ws
        self._sub_lock = asyncio.Lock()  # one batch of subscription messages on the wire at a time
        self._clock = clock or SystemClock()
        self._connect = connect or websockets.connect
        self._ping_s = ping_interval_s
        self._read_timeout_s = read_timeout_s
        self.connected = False
        self.connected_since: float | None = None  # when the current connection opened; None if down
        self.last_message_at = 0.0
        self.connects = 0
        self.pongs = 0
        self.errors = 0
        self.last_error: str | None = None
        self.last_error_at: float | None = None

    @property
    def users(self) -> dict[str, str]:
        """name -> address, the accounts followed now."""
        return dict(self._users)

    def _follow(self, users: Users) -> None:
        named = _named(users)
        names = list(named)
        self._users = {n: named[n] for n in names[:MAX_WS_USERS]}
        self._by_addr = {}
        for name, addr in self._users.items():  # one address under two names: the first one gets it
            self._by_addr.setdefault(addr.lower(), name)
        overflow = names[MAX_WS_USERS:]
        if overflow and overflow != self.overflow:
            log.warning("the websocket follows at most %d users: %d left out (see FillsFeed.overflow)",
                        MAX_WS_USERS, len(overflow))
        self.overflow = overflow

    def _wanted(self) -> dict[str, str]:
        """lowercase address -> address, for every followed account."""
        return {addr.lower(): addr for addr in self._users.values()}

    async def set_users(self, users: Users) -> None:
        """Follow these accounts from now on. A removed account's fills are dropped at once, even one
        already on its way. On a live connection, removed addresses are unsubscribed first (freeing
        per-IP user slots) and new ones subscribed; with none, the next connection subscribes the set."""
        self._follow(users)
        async with self._sub_lock:
            ws, live = self._ws, self._live
            if ws is None:
                return
            want = self._wanted()
            try:
                for low in [a for a in live if a not in want]:
                    await ws.send(_subscription("unsubscribe", live[low]))
                    live.pop(low)
                for low in [a for a in want if a not in live]:
                    await ws.send(_subscription("subscribe", want[low]))
                    live[low] = want[low]
            except Exception as e:  # noqa: BLE001 - the read loop sees a dead connection and reconnects
                log.warning("websocket subscription change failed (a reconnect subscribes the current set): %r", e)

    async def _ping_loop(self, ws) -> None:
        while True:
            await asyncio.sleep(self._ping_s)
            try:
                await ws.send(json.dumps({"method": "ping"}))
            except Exception as e:  # noqa: BLE001 - the read loop sees the dead connection and reconnects
                log.warning("ping send failed: %r", e)
                return

    def _handle(self, raw) -> list[Fill]:
        self.last_message_at = self._clock.now()
        try:
            msg = json.loads(raw)
        except ValueError:
            return []
        if not isinstance(msg, dict):
            return []
        channel = msg.get("channel")
        if channel == "pong":
            self.pongs += 1
            return []
        if channel == "error":  # never dropped silently
            self.errors += 1
            self.last_error = str(msg.get("data"))[:ERROR_TEXT_MAX]
            self.last_error_at = self._clock.now()
            log.warning("Hyperliquid websocket error: %s", self.last_error)
            return []
        if channel != "userFills":
            return []
        data = msg.get("data")
        if not isinstance(data, dict) or data.get("isSnapshot"):
            return []
        name = self._by_addr.get(str(data.get("user", "")).lower())
        if name is None:
            return []
        out = []
        for f in data.get("fills") or []:
            try:
                out.append(parse_fill(name, f))
            except (TypeError, KeyError, ValueError, OverflowError) as e:
                log.warning("skipping unparseable fill: %r", e)
        return out

    async def fills(self) -> AsyncIterator[Fill]:
        """New fills, forever. Cancel the task that iterates it to stop."""
        backoff = BACKOFF_START
        while True:
            ping_task = None
            try:
                async with self._connect(self._url, ping_interval=None) as ws:  # liveness is the app-level ping
                    self.connects += 1
                    async with self._sub_lock:  # a set_users meanwhile waits, then sends what changed
                        want = self._wanted()
                        for addr in want.values():
                            await ws.send(_subscription("subscribe", addr))
                        self._ws, self._live = ws, want
                    self.connected = True
                    self.connected_since = self._clock.now()
                    backoff = BACKOFF_START
                    ping_task = asyncio.create_task(self._ping_loop(ws))
                    while True:
                        try:  # real time, like the pings: recv() is safe to cancel
                            raw = await asyncio.wait_for(ws.recv(), self._read_timeout_s)
                        except TimeoutError:
                            log.warning("no websocket frame for %.0f s: reconnecting", self._read_timeout_s)
                            break  # leaving the `async with` closes it
                        for fill in self._handle(raw):
                            yield fill
            except ConnectionClosedOK:
                log.info("websocket closed by the server: reconnecting")
            except Exception as e:  # noqa: BLE001 - never raise out of the feed (CancelledError is BaseException)
                log.warning("websocket error: %r", e)
            finally:
                self._ws, self._live = None, {}
                self.connected = False
                self.connected_since = None
                if ping_task:
                    ping_task.cancel()
            await self._clock.sleep(backoff)
            backoff = min(backoff * 2, BACKOFF_MAX)
