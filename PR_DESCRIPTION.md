# Summary

This PR fixes several critical reliability, capital-management, and state-recovery issues in the Polymarket farming bot. It also improves the UI so manual orders, unmanaged positions, active bot positions, and historical orders are clearly separated.

The main safety fix is the new capital model: the bot no longer treats `free_balance + locked_usdc` as available capital. Instead it uses fixed `USDC на позицию`, a total bot capital limit, and current free balance only as a "can this order be placed now" check.

# Critical Bugs Fixed

## 1. Uninstallable Dependency

`polymarket-sdk>=0.1.0` was not available through pip, causing:

```text
ERROR: Could not find a version that satisfies the requirement polymarket-sdk>=0.1.0
```

The dependency was changed to:

```toml
"polymarket-client>=0.1.0b1"
```

## 2. Unsafe Capital Model

Old behavior:

```text
total_capital = free_balance + locked_usdc
slot_value = total_capital * slot_pct
```

This is dangerous because each newly opened order increases `locked_usdc`, which can increase the bot's calculated capital and create a feedback loop where the bot opens larger/more positions than intended.

New behavior:

- `order_usdc`: fixed USDC budget per bot entry.
- `bot_capital_limit_usdc`: total bot-managed BUY exposure limit.
- free balance is used only to check whether the next order can be placed.
- manual orders are not counted as bot capital.
- a 10% free-balance buffer is enforced.

## 3. Order Cost Could Exceed Configured Budget

The old sizing logic could fall back to `min_size` after computing an oversized order, but that `min_size` could still cost more than the configured budget.

The new logic has a final hard guard:

```text
if final_order_cost > order_budget:
    skip
```

## 4. No Crash-Safe Local Order Journal

If the bot placed an order on Polymarket but crashed before storing the real `order_id`, local state could be lost.

New behavior:

- create local `PENDING_PLACE` position before sending `place_limit`;
- replace local id with real Polymarket `order_id` after success;
- reconcile pending/open local records with live Polymarket orders on restart.

## 5. Restart Did Not Reliably Recover Open/Filled State

Startup now runs reconciliation:

- open orders are matched against `farm.db`;
- filled orders are marked `FILLED`;
- cancelled orders are marked `CANCELLED`;
- orphan filled BUY positions trigger missing SELL recovery.

## 6. Active Positions Included Inactive Orders

Cancelled/filled/failed old orders were shown in `Активные позиции`, which was misleading.

New behavior:

- active section shows only active/pending working records;
- inactive records are moved into a collapsible history block;
- active rows can expand to show prior entries for the same market.

## 7. Market Refresh Could Crash

`/api/markets/refresh` referenced missing config key `max_slots`, causing:

```text
KeyError: 'max_slots'
```

Refresh now uses the bot's current scanner config and `scan_once()`.

## 8. Scanner Could Block Trading Decisions

The old `tick` did scan, stale exits, capital checks, rebalancing, and entries sequentially. Long scans could delay order management.

New structure:

- `scanner_loop`: updates market candidates;
- `trader_loop`: makes trading decisions from current candidates;
- `monitor_loop`: tracks open orders/fills/repositioning.

The manual `Тик` button still runs one full scan+trade pass.

# Other Improvements

- Added manual open orders section.
- Added unmanaged positions section.
- Added actions:
  - cancel one manual order;
  - cancel all open orders;
  - cancel only bot-managed working orders;
  - take unmanaged position under bot control;
  - place SELL for unmanaged position.
- Added per-market order history expansion.
- Added global order history with pagination.
- Added scan status reporting:
  - total reward markets loaded;
  - markets passing min reward;
  - batch size;
  - scored count;
  - candidate pool size;
  - shown count;
  - cursor;
  - last update time.
- Added position fields:
  - `outcome`;
  - `parent_order_id`;
  - `local_id`;
  - `source`.
- Added balance snapshots and 24h balance chart data.
- Added `todo.md` with a planned Polymarket user WebSocket integration.

# Validation

The following checks were run locally:

```bash
.venv\Scripts\python.exe -m compileall src
node --check static\app.js
```

# Notes / Follow-Up

The bot still relies mainly on REST polling. A future PR should add:

- authenticated Polymarket user WebSocket for own order/trade events;
- global REST rate limiter;
- exponential backoff for `429`, `5xx`, and timeout responses;
- less aggressive monitor polling after WebSocket events are verified.
