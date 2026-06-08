# TODO: WebSocket / Scanner Follow-Up

## Runtime Architecture Notes

- `user_ws` is for fast private events about the bot's own orders.
- `market_ws` is for fast order book movement on active bot markets.
- REST stays in the system for commands, scanning, startup recovery, balances, and safety reconciliation.

REST cannot be removed because order placement/cancellation, startup recovery, balance checks, scans, and fallback reconciliation still need request/response APIs.

## User WS: Remaining Work

1. Observe real `trade` / fill payloads in `logs/ws-YYYY-MM-DD.log`.
2. Add explicit handling for failed/rejected WS trade/order statuses once real payloads are known.
3. Keep SELL creation centralized through `_recover_missing_sells()` so REST monitor and WS cannot create duplicate SELL orders for the same filled BUY.
4. Keep REST reconcile after startup/reconnect/missed events.
5. Reduce REST `get_order()` polling further only after WS fill payloads are verified in production logs.

## Market WS: Remaining Work

1. Add an operator/debug endpoint or command to inspect live cached book depth for a token.
2. Add optional low-rate WS-vs-REST parity sampling for diagnostics.
3. Keep REST fallback for stale/missing WS books, reconnects, placement/cancellation, startup recovery, balances, and safety reconciliation.

## Order Size Liquidity Filter

Add a filter so the bot does not become too large a share of the book liquidity at the place where it wants to quote.

Design options to choose from:

1. Target price level only:
   - compare order USDC against existing liquidity at the exact target price.
2. Reward-zone liquidity:
   - compare order USDC against total liquidity inside the active reward zone.
3. Top-N nearby levels:
   - compare order USDC against liquidity across the nearest N levels around target price.
4. Combined rule:
   - apply the strictest limit from target level, reward zone, and top-N levels.

Open questions:

- What max share should be allowed, for example 50%.
- Whether the filter should shrink the order or skip the market if the allowed size is below Polymarket minimum.
- Whether the same rule should apply differently for BUY entry orders and SELL recovery orders.

## REST Hardening

1. Add global REST rate limiting.
2. Add exponential backoff for `429`, `5xx`, timeouts, and transient connection errors.
3. Add round-robin monitor mode for large active position counts.

## Scanner / Multi Smart Discovery

Legacy remains the production-safe scanner for now. The current simple Multi strategy is faster per batch, but `rate_per_day DESC` can put the bot into highly competitive markets first and produce empty batches.

Build a smarter discovery layer instead of scanning Multi sequentially by reward rate.

1. Add a `multi_smart` or `gamma_fast` scanner mode.
2. Bulk-load a large discovery universe from a fast source:
   - `GET /rewards/markets/multi`; or
   - Gamma `/events?active=true&closed=false` flattened into markets, like `polymarket-scanner-main`.
3. Run cheap prefilters before any heavy CLOB/history calls:
   - category/tag blacklist;
   - word blacklist;
   - min daily reward;
   - rewards min size;
   - rewards max spread;
   - end date;
   - token prices;
   - rough volume/liquidity.
4. Build a balanced shortlist instead of taking the first 100:
   - high reward bucket;
   - mid reward bucket;
   - low/mid "gem" bucket;
   - min-size buckets such as 20/40/50;
   - random/unseen candidates so the scanner does not get stuck at the top of one sorted list.
5. Send only the shortlist into the expensive scorer:
   - live order books;
   - price history;
   - bid-depth spread;
   - reward-zone liquidity;
   - volatility/trade-count checks.
6. Add scan diagnostics for each stage:
   - raw loaded;
   - cheap prefilter passed;
   - shortlist size;
   - heavy scored;
   - reasons for most skips;
   - elapsed time per stage.
7. Keep Legacy as the fallback mode until the smart scanner consistently finds candidates at least as well as Legacy.
