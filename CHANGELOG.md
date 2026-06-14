# Changelog

## Unreleased

### Critical Fixes

- Replaced unavailable dependency `polymarket-sdk>=0.1.0` with installable `polymarket-client>=0.1.0b1`.
- Fixed wallet/balance handling by supporting the correct Polymarket wallet/safe address flow.
- Added startup/order reconciliation so local `farm.db` can recover state from Polymarket after app restart or crash.
- Added pre-place local journal entry with `PENDING_PLACE` status before sending a limit order to Polymarket.
- Added recovery for orders that were accepted remotely but not fully written locally before a crash.
- Added recovery for filled BUY positions that have no corresponding SELL order.
- Fixed a dangerous capital model bug where `free_balance + locked_usdc` was used as total capital, causing bot buying power to grow as more orders were opened.
- Replaced percentage-based position sizing with fixed `USDC на позицию` plus `Макс. капитал бота, USDC`.
- Stopped manual Polymarket orders from increasing bot capital allocation.
- Added final order-cost guard so reward `min_size` cannot force an order above configured per-position budget.
- Split long synchronous bot tick into separate scanner, trader, and monitor async loops so active order management is not blocked by market scanning.
- Prevented empty scan results at startup/API failure from being treated as a reason to close all active positions.
- Added daily log rotation under `logs/` so runtime logs no longer grow into one unbounded `bot.log`.

### Position Management

- Added manual open orders view.
- Added cancellation for a single manual order.
- Added `Отменить все` to cancel all open Polymarket orders.
- Added `Отменить рабочие` to cancel only bot-managed working orders.
- Added unmanaged positions view for positions opened manually or left outside bot control.
- Added actions for unmanaged positions:
  - `Взять под контроль`
  - `Выставить SELL`
- Added automatic reconciliation endpoint and startup reconciliation.
- Added local history tracking for old/cancelled/filled orders.
- Moved inactive orders out of `Активные позиции` into a collapsible history section.
- Added expandable per-market history under active positions.
- Added private Polymarket user WebSocket monitoring for bot order events.
- Added dynamic user WebSocket subscriptions for active bot markets, with delayed unsubscribe.
- Added safe WebSocket-driven local cancellation updates for `CANCELLATION/CANCELED` order events.
- Added market ban actions so an active position or candidate market can be cancelled/hidden for 24 hours without immediately re-entering it.

### UI Improvements

- Renamed `% на позицию` to `USDC на позицию`.
- Added `Макс. капитал бота, USDC`.
- Added client-side max check so `USDC на позицию` cannot exceed 90% of current free balance.
- Updated active positions table to include market name, outcome, side, price, size, USDC, status, placed time, and action.
- Added manual orders and unmanaged positions sections.
- Added collapsible history with pagination.
- Added scan status text showing loaded reward markets, filtered markets, batch size, scored count, pool size, shown count, cursor, and update time.
- Improved caret buttons for expandable rows/sections.
- Added controls for target-level share protection and confirmation delay.
- Added UI actions for banning a position's market and banning a market candidate.

### Scanner / Trading Logic

- Fixed `/api/markets/refresh` crash caused by missing `max_slots` key.
- Added buy-side bid depth filter (`max_bid_depth_spread`) so markets with fewer than 4 bid levels or a large gap between bid levels 1-4 on the token the bot will actually buy are skipped/cancelled as thin books.
- Added target-price level share protection (`max_target_level_share_pct`) so the bot does not become too large a share of the exact bid level it quotes.
- Added delayed confirmation for target-level share breaches before shrinking or cancelling an oversized live BUY.
- Added cautious top-up logic when a live BUY level can fit more size under the configured max share.
- Changed market candidate filtering to use fixed `order_usdc` budget.
- Removed oversized "super-deal" multi-slot order sizing; one entry now uses one fixed position budget.
- Kept rebalance behavior, but reinterpreted `max_slots_per_market` as max cancellations per rebalance instead of allowing larger order size.
- Added safer capital checks:
  - free balance is used only as "can we open now";
  - bot capital limit is calculated from bot-managed exposure;
  - manual orders do not affect bot exposure.
- Added a logging-only WebSocket probe for validating private user events and active-market order book streams.
- Added active-position market WebSocket book cache with dynamic subscribe/unsubscribe and REST fallback for stale/missing books.
- Changed active order monitoring to prefer fresh WebSocket books before REST order book reads.
- Added fast WebSocket-triggered BUY step-down for `mid`/`edge` depth modes when an open order becomes the best bid.
- Kept size changes, top-ups, exits, and SELL management in the regular monitor loop to avoid overreacting to noisy book flicker.
- Split candidate handling into trade candidates and UI-shown candidates so the UI reflects the currently displayable market set without losing the scanner's larger internal pool.

### Data Model

- Added `outcome`, `parent_order_id`, `local_id`, and `source` fields to `positions`.
- Added active/history status grouping.
- Added bot-managed capital exposure calculation.
- Added balance snapshots for 24h chart/statistics.
- Added `market_bans` table for temporary market-level exclusions.

### Future Work

- Add global REST rate limiter and exponential backoff for `429`, `5xx`, and timeout responses.
- Add WebSocket fill/trade handling after a real fill payload is observed.
- Reduce REST polling after WebSocket fill handling is verified.
- Add round-robin monitor mode for large position counts.
- Add an operator/debug endpoint for inspecting cached WebSocket book depth and low-rate REST/WS parity samples.
- Add smarter multi/gamma discovery so the scanner can cheaply prefilter a broad universe before expensive CLOB/history scoring.
