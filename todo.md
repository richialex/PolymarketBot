# TODO: WebSocket / Scanner Follow-Up

## Runtime Architecture Notes

- `user_ws` is used for fast private events about the bot's own orders.
- `market_ws` is used for fast order book movement on active bot markets.
- REST remains required for order placement/cancellation, scanning, startup recovery, balances, and safety reconciliation.
- Current order management prefers fresh WebSocket books for active positions and falls back to REST when WS data is stale or missing.

## User WS: Remaining Work

1. Add explicit handling for failed/rejected WS trade/order statuses once real payloads are observed.
2. Keep SELL creation centralized through `_recover_missing_sells()` so REST monitor and WS cannot create duplicate SELL orders for the same filled BUY.
3. Keep REST reconcile after startup, reconnect, and missed events.
4. Further reduce REST `get_order()` polling now that matched order updates and trade payloads have been observed in production logs.

## Market WS: Remaining Work

1. Add an operator/debug endpoint or command to inspect live cached book depth for a token.
2. Add optional low-rate WS-vs-REST parity sampling for diagnostics.
3. Keep REST fallback for stale/missing WS books, reconnects, placement/cancellation, startup recovery, balances, and safety reconciliation.
4. Continue observing `FAST_STEP` behavior in production to confirm it catches front-of-queue BUY drift before the regular monitor cycle.

## Order Size Liquidity Filter

Implemented:

- Target-price level share protection via `max_target_level_share_pct`.
- Confirmation delay via `target_level_share_confirm_s`.
- Shrink/cancel behavior when a live BUY exceeds the configured target-level share.
- Cautious top-up behavior when an existing level can fit more size under the configured share.

Remaining ideas:

1. Reward-zone liquidity:
   - compare order USDC against total liquidity inside the active reward zone.
2. Top-N nearby levels:
   - compare order USDC against liquidity across the nearest N levels around target price.
3. Combined rule:
   - apply the strictest limit from target level, reward zone, and top-N levels.
4. Decide whether SELL recovery orders need a separate share/liquidity rule.

## REST Hardening

1. Add global REST rate limiting.
2. Add exponential backoff for `429`, `5xx`, timeouts, and transient connection errors.
3. Further reduce non-critical REST polling now that user/order WebSocket handling is proven.
4. Add round-robin monitor mode for large active position counts.

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

## Upstream Changes To Evaluate

Upstream `ggRonin/PolymarketBot` added commit `d73fb76` with several small scanner/trading changes. Do not merge blindly; port the useful pieces into the refactored architecture.

1. Add configurable minimum days to expiry:
   - upstream exposes `min_days_to_expiry`;
   - current refactor still has `30` days hardcoded in `scanner.enrich_batch()`;
   - useful and low risk to port.
2. Fix `edge` depth price improvement:
   - upstream disables second-level order-book improvement when `depth == "edge"`;
   - current refactor still lets edge mode move inward to the second bid level;
   - useful if edge mode should truly stay near the outer reward-zone edge.
3. Add live spread check immediately before entry:
   - upstream checks live `max_ob_spread` before placing;
   - current refactor already checks live bid depth, best ask crossing, and target-level share before entry, but does not re-check live bid/ask spread at placement time;
   - useful minor safety port.
4. Revisit buy-token selection:
   - upstream switched from cheaper token to more expensive token after observing fewer fills in practice;
   - current refactor still buys the cheaper valid token;
   - this changes exposure and min-size economics, so prefer making it configurable or testing before changing the default.
