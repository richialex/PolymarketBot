# TODO

## REST Hardening

1. Add a global REST rate limiter.
2. Add exponential backoff for `429`, `5xx`, timeouts, and transient connection errors.
3. Further reduce non-critical REST polling now that user/order WebSocket handling is proven.
4. Split the fast position monitor interval from the slower `trade_once` cadence so WS protection can stay quick while balance/rebalance REST calls run at a calmer 15-30s interval.
5. Add round-robin monitor mode for large active position counts.

## WebSocket Follow-Up

1. Add explicit handling for failed/rejected WS trade/order statuses once real payloads are observed.
2. Add an operator/debug endpoint or command to inspect live cached book depth for a token.
3. Add optional low-rate WS-vs-REST parity sampling for diagnostics.
4. Continue observing `FAST_STEP` behavior in production to confirm it catches front-of-queue BUY drift before the regular monitor cycle.

## Liquidity / Size Rules

1. Add optional reward-zone liquidity share protection.
2. Add optional Top-N nearby-level liquidity share protection.
3. Consider a combined rule that applies the strictest limit from target level, reward zone, and Top-N levels.
4. Decide whether SELL recovery orders need a separate share/liquidity rule.

## Scanner / Discovery

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

## Upstream Changes To Port Or Decide

1. Add configurable `min_days_to_expiry`; current refactor still has `30` days hardcoded in `scanner.enrich_batch()`.
2. Fix `edge` depth price improvement so edge mode does not move inward to the second bid level unless explicitly desired.
3. Add live `max_ob_spread` check immediately before entry.
4. Decide whether buy-token selection should remain cheaper-token, switch to expensive-token, or become configurable.
