# TODO: Polymarket User WebSocket

## Goal

Reduce REST polling and react faster to own order/trade events by adding Polymarket authenticated user WebSocket as an event source.

## Plan

1. Add a separate `user_ws_loop` task to `FarmingBot.start()`.
2. On startup, build the subscription market list from active/reconcilable bot positions in `farm.db`.
3. Connect to Polymarket user WebSocket with API credentials.
4. First phase: only log incoming `order` and `trade` events without changing DB state.
5. Add reconnect handling:
   - reconnect on disconnect;
   - run `reconcile()` after reconnect;
   - use backoff after repeated failures.
6. Add event-to-DB mapping:
   - matched/filled BUY -> `FILLED`;
   - cancelled order -> `CANCELLED`;
   - open/live order update -> keep/update `OPEN`.
7. Trigger existing SELL recovery logic when a BUY fill event arrives.
8. Keep REST reconcile as fallback every few minutes.
9. After WS proves stable, reduce polling in `monitor_loop` or switch order checks to round-robin.

## Subscription Rules

- Do not subscribe to all markets.
- Subscribe only to markets with active bot-managed orders or filled bot-managed BUY positions.
- Update subscription list slowly, for example every 30-60 seconds.
- Keep recently closed markets subscribed for a short TTL, for example 5-10 minutes, to avoid constant resubscribe churn.

## Safety

- Do not place or cancel orders from WS events until event format is verified in logs.
- Always keep REST reconciliation as source-of-truth recovery after restart or WS disconnect.
- Add rate limiting/backoff around reconnect and REST fallback calls.

# TODO: Log Rotation

## Goal

Stop `bot.log` from growing into one huge file and make debugging by date easier.

## Plan

1. Move runtime logs into a dedicated `logs/` directory.
2. Rotate logs by date, for example `logs/bot-YYYY-MM-DD.log`.
3. Keep the current day writable and preserve old daily logs for debugging.
4. Add retention cleanup, for example keep 14-30 days and delete or compress older files.
5. Keep console logging unchanged.
