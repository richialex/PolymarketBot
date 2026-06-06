# TODO: WebSocket Follow-Up

## User WS Fill Handling

The private `user` WebSocket is already running in the bot and safely handles `CANCELLATION/CANCELED -> CANCELLED`.

Remaining work:

1. Observe a real `trade` / fill payload in `logs/ws-YYYY-MM-DD.log`.
2. Add idempotent event handling for fills:
   - partial fill -> record or track matched size without closing the order;
   - full BUY fill -> mark local BUY as `FILLED`;
   - failed/rejected order -> mark local order as `FAILED`.
3. Trigger existing SELL recovery logic when a full BUY fill arrives.
4. Guard against duplicate SELL creation if REST monitor and WS see the same fill.
5. Keep REST reconcile as fallback after startup, reconnect, and missed events.
6. After fill handling is stable, reduce REST `get_order()` polling in `monitor_loop`.

## Market WS For Active Positions

Market WebSocket was verified with `scripts/ws_probe.py`, but it is not yet part of the bot because `price_change` is noisy and needs a correct book cache.

Future work:

1. Add a separate `market_ws_loop` only for active bot positions.
2. Subscribe by token IDs (`assets_ids`), not all scanned candidate markets.
3. Keep a runtime registry:
   - `condition_id -> token_ids`;
   - `token_id -> latest book/best_bid/best_ask`;
   - `condition_id -> unsubscribe_after`.
4. Use a 60 second delayed unsubscribe when a market disappears.
5. Maintain book state from `book`, `price_change`, and optionally `best_bid_ask` events.
6. Validate WS book state against REST before using it for order placement decisions.
7. Keep REST `get_order_book()` fallback for stale/missing WS data and after reconnect.

## REST Hardening

1. Add global REST rate limiting.
2. Add exponential backoff for `429`, `5xx`, timeouts, and transient connection errors.
3. Add round-robin monitor mode for large active position counts.
