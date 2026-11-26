# Hyperliquid read API: checked facts

Notes on Hyperliquid's read-only API (`POST /info` and websocket subscriptions), checked on 22 Nov 2026 against
the official GitBook docs, the official Python SDK 0.24.0 source, and live read-only `/info` calls to testnet and
mainnet.

Short names used below:
- `DOCS` = https://hyperliquid.gitbook.io/hyperliquid-docs
- `API` = `DOCS`/for-developers/api
- `SDK` = https://github.com/hyperliquid-dex/hyperliquid-python-sdk

**UNCONFIRMED** marks anything the docs or source do not state outright. Hyperliquid changes; check anything
you depend on.

---

## 1. Endpoints

| | Mainnet | Testnet |
| --- | --- | --- |
| REST | `https://api.hyperliquid.xyz/info` | `https://api.hyperliquid-testnet.xyz/info` |
| Websocket | `wss://api.hyperliquid.xyz/ws` | `wss://api.hyperliquid-testnet.xyz/ws` |

- Query with the **account's own address**. Docs: an API wallet's address "leads to an empty result".
- The official SDK's `Info.__init__` makes blocking `requests` calls (`spotMeta`, `perpDexs`, `meta` per dex) and its
  websocket manager is thread-based (`websocket-client`). Avoid constructing it inside asyncio; this toolkit exists
  for that reason.

## 2. Rate limits

Source: `API/rate-limits-and-user-limits`.

**REST, per IP:** an aggregated weight of **1200 per minute**.

**Info weights**
- Weight **2**: `l2Book, allMids, clearinghouseState, orderStatus, spotClearinghouseState, exchangeStatus`.
- Weight **60**: `userRole`.
- Weight **20**: every other documented info request, including `openOrders`, `frontendOpenOrders`, `meta`, `userFees`.
- Extra weight **per 20 items returned**, on top of the base: `userFills, userFillsByTime, userFunding,
  historicalOrders, recentTrades, fundingHistory, nonUserFundingUpdates, twapHistory, userTwapSliceFills(ByTime),
  delegatorHistory, delegatorRewards, validatorStats`.
- `candleSnapshot` adds weight per **60** items.
- Explorer requests weigh 40 each.

**Websocket, per IP**
- max **10** connections
- **30** new connections per minute
- **1000** subscriptions
- **10 unique users** across user-specific subscriptions. Past that, a subscribe gets an `error` channel frame
  (for example "Cannot track more than 10 total users."), not fills. It is easy to miss if error frames are ignored.
- **2000** messages sent per minute
- **100** simultaneous inflight `post` messages

**Per address (orders and other actions only, not reads).** Each address starts with a buffer of 10000 requests and
earns 1 more per 1 USDC traded. `{"type": "userRateLimit", "user"}` returns `cumVlm, nRequestsUsed, nRequestsCap,
nRequestsSurplus`. "Portfolio margin and unified account are limited to 50k user actions per day."

## 3. Fills, funding and paging

**Fills** (`API/info-endpoint`)
- `{"type": "userFills", "user", "aggregateByTime"?: bool}` returns at most the 2000 most recent fills. It includes
  HIP-3 fills (`"coin": "xyz:XYZ100"`); there is no `dex` param.
- `{"type": "userFillsByTime", "user", "startTime" (ms, inclusive), "endTime"?, "aggregateByTime"?}` returns at most
  2000 per response, and only the 10000 most recent fills are reachable.
- **Paging:** use the last row's timestamp as the next `startTime`. The general info page says time-range responses
  return 500 elements; the fills page says 2000. **Handle either.** Restart *at* the last millisecond, not after it,
  or fills that share it are lost; drop the repeats by `tid`. A full page that is all one millisecond cannot be paged
  past by time.
- Fill fields: `coin, px, sz, side ("B"/"A"), time, startPosition, dir (e.g. "Open Long", "Sell"), closedPnl, hash,
  oid, crossed (taker), fee, feeToken, tid`.
  - Optional `builderFee`. `fee` is "the total fee, inclusive of builderFee". The websocket type says a negative
    `fee` is a rebate.
  - Optional `liquidation` (section 7).
  - Fills carry `oid` but **no cloid**.
- Prediction-market (HIP-4) outcome coins look like `"#165420"` and can settle at a price of 0. A parser that
  refuses `px <= 0` should skip them rather than fail the whole page.

**Funding**
- `{"type": "userFunding", "user", "startTime", "endTime"?}` returns `[{"delta": {"type": "funding", "coin",
  "usdc", "szi", "fundingRate", "nSamples"}, "hash", "time"}]`, HIP-3 coins included.
- `{"type": "fundingHistory", "coin", "startTime", "endTime"?}` returns a market's rates: `[{coin, fundingRate,
  premium, time}]`.
- `userFees` gives fee rates (weight 20).

## 4. Orders and positions (reading them)

**`orderStatus`:** `{"type": "orderStatus", "user", "oid": <u64> | "<0x cloid>"}`. The SDK's `query_order_by_cloid`
sends the cloid in the `oid` field.
- Found: `{"status": "order", "order": {"order": {coin, side, limitPx, sz, oid, timestamp, triggerCondition,
  isTrigger, triggerPx, children, isPositionTpsl, reduceOnly, orderType, origSz, tif, cloid}, "status": <status>,
  "statusTimestamp"}}`.
- Not found: `{"status": "unknownOid"}`.
- Status strings:
  - `open, filled, canceled, triggered, rejected`
  - `marginCanceled, vaultWithdrawalCanceled, openInterestCapCanceled, selfTradeCanceled, reduceOnlyCanceled,
    siblingFilledCanceled, delistedCanceled, liquidatedCanceled, scheduledCancel`
  - `tickRejected, minTradeNtlRejected, perpMarginRejected, reduceOnlyRejected, badAloPxRejected, iocCancelRejected,
    badTriggerPxRejected, marketOrderNoLiquidityRejected, positionIncreaseAtOpenInterestCapRejected,
    positionFlipAtOpenInterestCapRejected, tooAggressiveAtOpenInterestCapRejected, openInterestIncreaseRejected,
    insufficientSpotBalanceRejected, oracleRejected, perpMaxPositionRejected`

**Open orders**
- `{"type": "openOrders", "user", "dex"?}` gives `[{coin, limitPx, oid, side, sz, timestamp}]`.
- `{"type": "frontendOpenOrders", "user", "dex"?}` adds `isPositionTpsl, isTrigger, orderType, origSz, reduceOnly,
  triggerCondition, triggerPx` (and `cloid` per the SDK docstring).
- **Both are per dex.** `dex` defaults to `""` (the main dex). Query each HIP-3 dex separately.

**Positions:** `clearinghouseState` fields are `assetPositions[].position{coin, cumFunding{allTime, sinceChange,
sinceOpen}, entryPx, leverage{type, value, rawUsd?}, liquidationPx, marginUsed, maxLeverage, positionValue,
returnOnEquity, szi, unrealizedPnl}, crossMaintenanceMarginUsed, crossMarginSummary, marginSummary{accountValue,
totalMarginUsed, totalNtlPos, totalRawUsd}, time, withdrawable`. It is per dex: ask with `dex: ""` and again with
`dex: "xyz"`.

## 5. Asset ids and HIP-3 dexes

Source: `API/asset-ids`.

- **Main-dex perps:** the asset id is the index in `meta.universe` ("BTC = 0 on mainnet").
- **Builder-deployed (HIP-3) perps:** `100000 + perp_dex_index * 10000 + index_in_meta`. Names are always
  `"{dex}:{coin}"`. Docs example: `test:ABC` on testnet has `perp_dex_index = 1`, asset `110000`.
- **Spot:** `10000 + spotMeta.universe index`.
- `perp_dex_index` is the position in the `{"type": "perpDexs"}` array. Element 0 is `null` (the main dex).
- Info calls that take `dex`: `clearinghouseState`, `openOrders`, `frontendOpenOrders`, `allMids`, `meta`,
  `metaAndAssetCtxs`. `userFills` has none.

**Mainnet and testnet differ** (live calls, 22 Nov 2026):

| | Main-dex `meta` | `perpDexs` | `xyz` dex |
| --- | --- | --- | --- |
| Mainnet | BTC=0, ETH=1 (235 entries) | 11 entries | index **1**: `xyz:XYZ100`=110000, `xyz:NVDA`=110002, `xyz:GOLD`=110003 |
| Testnet | SOL=0, APT=1, ATOM=2, **BTC=3**, ETH=4 (213 entries) | 288 entries | index **65**: `xyz:XYZ100`=750000, `xyz:NVDA`=750002, `xyz:GOLD`=750003 |

**Never hard-code ids.** Build them from `perpDexs` and `meta` on each network at startup. Testnet also has
look-alike dexes whose full name is "XYZ" (`x`, `t`, `z`, `y`): **match on `name == "xyz"`.**

**Isolated-only HIP-3 markets** (`meta`): `marginMode` is `"strictIsolated"` (margin cannot be removed) or
`"noCross"` (isolated only); `onlyIsolated: true` is deprecated and means either. Which markets are which differs
between networks, so read `marginMode` at runtime. Each dex's meta also has `collateralToken`, `growthMode`,
`deployerFeeScale` and `isDelisted`.

## 6. Tick and lot rules

Source: `API/tick-and-lot-size`. Useful for checking prices you read or show.

- Prices: "up to 5 significant figures, but no more than `MAX_DECIMALS - szDecimals` decimal places where
  `MAX_DECIMALS` is 6 for perps and 8 for spot. Integer prices are always allowed, regardless of the number of
  significant figures."
- Examples: `1234.5` is valid, `1234.56` is not. `0.001234` is valid, `0.0012345` is not. With szDecimals=1,
  `0.01234` is valid and `0.012345` is not.
- Sizes are rounded to the asset's `szDecimals`, from `meta`.
- The SDK's `examples/rounding.py`: if `px > 100_000` use `round(px)`, else `round(float(f"{px:.5g}"), 6 -
  szDecimals)`; `sz = round(sz, szDecimals)`.
- Minimum order value is $10.

## 7. Liquidations

Sources: `API/websocket/subscriptions`, `DOCS/trading/liquidations`.

- **Fills:** `WsFill` has an optional `liquidation` = `{liquidatedUser?, markPx, method: "market" | "backstop"}`.
  It is documented on the websocket types; **that REST fills carry it too is UNCONFIRMED.**
- **The `dir` text of a liquidation fill is UNCONFIRMED.** Use the `liquidation` field, not `dir`.
- **userEvents** (channel `"user"`): `{"liquidation": {lid, liquidator, liquidated_user, liquidated_ntl_pos,
  liquidated_account_value}}`.
- **userNonFundingLedgerUpdates** (REST or websocket): `delta.type == "liquidation"` with `{accountValue,
  leverageType: "Cross"|"Isolated", liquidatedPositions: [{coin, szi}]}`.
- Orders cancelled by a liquidation get status `liquidatedCanceled`.
- Positions over 100k USDC (10k on testnet) are liquidated 20% at a time, with a 30 s cooldown.

## 8. Account modes

Source: `DOCS/trading/account-abstraction-modes`.

- `{"type": "userAbstraction", "user"}` returns `"unifiedAccount" | "portfolioMargin" | "disabled" | "default" |
  "dexAbstraction"`. `"disabled"` is the standard (manual) mode: separate perp and spot balances, separate per dex.
- In unified and portfolio-margin modes, "individual perp dex user states are not meaningful": read equity from
  **`spotClearinghouseState.balances[]`** (`{coin, token, hold, total, entryNtl}`). Positions still come from
  `clearinghouseState` per dex.

## 9. Websocket subscriptions

Source: `API/websocket/subscriptions`.

Subscribe with `{"method": "subscribe", "subscription": {...}}`; the ack is `{"channel": "subscriptionResponse", …}`.
Unsubscribe the same way with `"method": "unsubscribe"`.

**Heartbeat:** the server closes a connection that has sent it nothing for 60 s. Send `{"method": "ping"}`; the
answer is `{"channel": "pong"}`. A connection can also die without closing (a half-open TCP connection after a
laptop sleeps); only a read timeout catches that.

- **`userFills`** `{"type": "userFills", "user", "aggregateByTime"?}` gives `{isSnapshot?, user, fills: WsFill[]}`.
  - The first message is a snapshot with `isSnapshot: true`. Dedupe by `tid`.
  - Docs: "Missed data during the reconnect will be present in the snapshot ack on reconnect."
- **`userEvents`** `{"type": "userEvents", "user"}`. **The channel name is `"user"`.** Data is one of
  `{"fills": WsFill[]}`, `{"funding": …}`, `{"liquidation": …}` or `{"nonUserCancel": [{coin, oid}]}`.
- **`orderUpdates`** `{"type": "orderUpdates", "user"}` gives `[{order: {coin, side, limitPx, sz, oid, timestamp,
  origSz, cloid}, status, statusTimestamp}]`. Whether it covers every dex is **UNCONFIRMED** (it has no dex param).
- **`clearinghouseState`** `{"type": "clearinghouseState", "user", "dex"}`.
- **`allDexsClearinghouseState`** `{"type": "allDexsClearinghouseState", "user"}` gives `{user, clearinghouseStates:
  [[dex, state], …]}`: every dex in one subscription.
- **`spotState`** `{"type": "spotState", "user", "isPortfolioMargin"?}` gives `{user, spotState: {balances: […]}}`.
- **`openOrders`** `{"type": "openOrders", "user", "dex"}`.
- `userFundings`, `userNonFundingLedgerUpdates`, `activeAssetData` and `notification` also exist.
- **`webData2` is no longer in the docs.** The list has **`webData3`** `{"type": "webData3", "user"}`, and the docs
  warn "Additional undocumented fields in WebData3 will be removed". SDK 0.24.0 still supports only `webData2`;
  **whether `webData2` still works is UNCONFIRMED.** Don't build on it.
- **Info requests over the websocket:** `{"method": "post", "id": n, "request": {"type": "info", "payload": {...}}}`.
  The answer is `{"channel": "post", "data": {"id", "response": {"type": "info", "payload"}}}`.
