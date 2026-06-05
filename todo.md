# TODO: Polymarket User WebSocket

## Goal

Reduce REST polling and react faster to own order/trade events by adding Polymarket authenticated user WebSocket as an event source.

The goal is not to subscribe to market order books. The bot only needs private user events such as own order updates, fills/trades, cancellations, and rejects so it can stop asking REST "filled yet?" every few seconds.

## Plan

1. Add a separate `user_ws_loop` task to `FarmingBot.start()`.
2. Connect to Polymarket authenticated user WebSocket with CLOB API credentials.
3. Subscribe to the private user event stream, for example `clob_user` `order` and `trade` events.
4. First phase: only log incoming `order` and `trade` events without changing DB state.
5. Add reconnect handling:
   - reconnect on disconnect;
   - run `reconcile()` after reconnect;
   - use backoff after repeated failures.
6. Add event-to-DB mapping:
   - partial fill -> record/update matched size and keep remaining order active;
   - matched/filled BUY -> `FILLED`;
   - cancelled order -> `CANCELLED`;
   - rejected/failed order -> `FAILED`;
   - open/live order update -> keep/update `OPEN`.
7. Trigger existing SELL recovery logic when a BUY fill event arrives.
8. Keep REST reconcile as fallback every few minutes.
9. After WS proves stable, reduce REST polling in `monitor_loop`; keep order-book REST calls only for repricing/risk checks.

## Subscription Rules

- Do not subscribe to public market/order-book streams for this task.
- Use the authenticated user channel only.
- Do not maintain per-market subscription lists unless Polymarket requires it for user events.
- Keep REST polling as a fallback after WS disconnects, startup, or missed events.

## Safety

- Do not place or cancel orders from WS events until event format is verified in logs.
- Always keep REST reconciliation as source-of-truth recovery after restart or WS disconnect.
- Add rate limiting/backoff around reconnect and REST fallback calls.
- Treat partial fills carefully, especially dust below Polymarket minimum SELL size.

# TODO: Log Rotation

## Goal

Stop `bot.log` from growing into one huge file and make debugging by date easier.

## Plan

1. Move runtime logs into a dedicated `logs/` directory.
2. Rotate logs by date, for example `logs/bot-YYYY-MM-DD.log`.
3. Keep the current day writable and preserve old daily logs for debugging.
4. Add retention cleanup, for example keep 14-30 days and delete or compress older files.
5. Keep console logging unchanged.
