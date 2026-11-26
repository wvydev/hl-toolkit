# hl-toolkit

Read-only [Hyperliquid](https://hyperliquid.xyz) clients for Python asyncio.

- **`FillsFeed`**: a websocket feed of accounts' fills that reconnects on its own (backoff, app-level ping,
  and it notices a dead half-open connection that never closes).
- **`InfoClient`**: the `POST /info` API with Hyperliquid's rate-limit weights built in, paging that doesn't lose
  rows, and retries. A busy program waits for room instead of getting HTTP 429.
- **Untrusted numbers are checked.** `float("NaN")` passes every `x > 0` test because NaN compares False both ways.
  Every price and size that comes back has been checked, and the same checks (`finite`, `positive`, `whole`) are
  yours to use.

Nothing here signs or sends orders, and it needs no keys.

## Install

```sh
uv add hl-toolkit        # or: pip install hl-toolkit
```

Python 3.11+. Depends only on `httpx` and `websockets`.

## Example

```python
import asyncio

from hltoolkit import FillsFeed, InfoClient

ACCOUNT = "0x..."  # any Hyperliquid account address


async def main():
    async with InfoClient() as info:
        mids = await info.all_mids()
        print("BTC mid:", mids["BTC"])
        candles = await info.candles("ETH", "1h", start_ms=1_700_000_000_000, end_ms=1_700_086_400_000)
        print(len(candles), "hourly ETH candles")

    feed = FillsFeed({"watched": ACCOUNT})
    async for fill in feed.fills():  # runs forever; cancel the task to stop
        print(fill.user, fill.coin, fill.side, fill.sz, "@", fill.px)


asyncio.run(main())
```

## Things worth knowing

- One IP may follow at most **10 accounts** over websockets. `FillsFeed` subscribes the first 10 and lists the rest
  in `feed.overflow`. Change who is followed with `await feed.set_users(...)` while it runs.
- Fills that happen while the feed is reconnecting are not replayed. Fill the gap with `InfoClient.fills_by_time`.
- REST calls share **1200 weight per minute per IP**. Share one `WeightLimiter` between clients on the same IP.
  `fills_by_time(..., keep_free=300)` lets a long history read run in the background without crowding out
  your other calls.
- Testnet: `InfoClient(TESTNET_INFO_URL)` and `FillsFeed(users, TESTNET_WS_URL)`.
- For tests, pass your own `clock` (anything with `now()` and `async sleep()`) and your own `httpx.AsyncClient`
  (for example with `httpx.MockTransport`).

[docs/api-facts.md](docs/api-facts.md) collects checked notes on Hyperliquid's read API: weights, paging caps,
websocket limits and the parts the official docs leave unclear.

## Develop

```sh
uv sync
uv run pytest
uv run ruff check .
```

The tests run offline.

## Licence

MIT. See [LICENSE](LICENSE).
