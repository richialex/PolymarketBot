from __future__ import annotations

import asyncio
import json
import time
from typing import Any

import websockets

from src import db
from src.logging_setup import configure_ws_file_logger
from src.pm_client import client
from src.order_manager import order_manager


USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
PING_INTERVAL_S = 10
REFRESH_INTERVAL_S = 5
UNSUBSCRIBE_DELAY_S = 60

log = configure_ws_file_logger("user_ws")


class UserWsWatcher:
    def __init__(self) -> None:
        self._seen_events: set[tuple[str, str, str]] = set()
        self._connected = False
        self._subscribed_markets: set[str] = set()
        self._last_message_at = 0.0
        self._last_pong_at = 0.0
        self._connected_since = 0.0

    async def run(self) -> None:
        backoff = 1
        while True:
            try:
                markets = await self._active_markets()
                if not markets:
                    log.info("USER_WS idle: no active markets")
                    self._set_connection_state(False, set())
                    await asyncio.sleep(REFRESH_INTERVAL_S)
                    continue
                await self._run_connection(markets)
                backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("USER_WS reconnect after error: %s", e)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def _run_connection(self, initial_markets: set[str]) -> None:
        auth = client.ws_auth()
        subscribed = set(initial_markets)
        pending_unsub: dict[str, float] = {}
        stop = asyncio.Event()

        sub = {"auth": auth, "markets": sorted(subscribed), "type": "user"}
        safe_key = auth.get("apiKey", "")
        log.info("USER_WS connecting markets=%d api_key=%s", len(subscribed), _short(safe_key))

        async with websockets.connect(USER_WS_URL, ping_interval=None) as ws:
            self._set_connection_state(True, subscribed)
            await ws.send(json.dumps(sub))
            log.info("USER_WS subscribed initial markets=%d", len(subscribed))

            tasks = [
                asyncio.create_task(self._heartbeat(ws, stop)),
                asyncio.create_task(self._subscription_loop(ws, stop, subscribed, pending_unsub)),
                asyncio.create_task(self._receive_loop(ws, stop)),
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                stop.set()
                self._set_connection_state(False, set())
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _heartbeat(self, ws, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(PING_INTERVAL_S)
            if stop.is_set():
                return
            await ws.send("PING")

    async def _receive_loop(self, ws, stop: asyncio.Event) -> None:
        while not stop.is_set():
            msg = await ws.recv()
            if msg == "PONG":
                self._mark_pong()
                continue
            self._mark_message()
            try:
                event = json.loads(msg)
            except json.JSONDecodeError:
                log.warning("USER_WS raw non-json %s", _clip(str(msg)))
                continue
            await self._handle_event(event)

    async def _subscription_loop(self, ws, stop: asyncio.Event, subscribed: set[str], pending_unsub: dict[str, float]) -> None:
        while not stop.is_set():
            await asyncio.sleep(REFRESH_INTERVAL_S)
            active = await self._active_markets()
            now = time.monotonic()

            for market in sorted(active - subscribed):
                await ws.send(json.dumps({"markets": [market], "operation": "subscribe"}))
                subscribed.add(market)
                self._set_subscribed_markets(subscribed)
                pending_unsub.pop(market, None)
                log.info("USER_WS subscribe market=%s total=%d", _short(market), len(subscribed))

            for market in active:
                pending_unsub.pop(market, None)

            for market in subscribed - active:
                pending_unsub.setdefault(market, now + UNSUBSCRIBE_DELAY_S)

            ready = sorted(m for m, deadline in pending_unsub.items() if deadline <= now)
            for market in ready:
                await ws.send(json.dumps({"markets": [market], "operation": "unsubscribe"}))
                subscribed.discard(market)
                self._set_subscribed_markets(subscribed)
                pending_unsub.pop(market, None)
                log.info("USER_WS unsubscribe market=%s total=%d", _short(market), len(subscribed))

            if not subscribed:
                log.info("USER_WS no subscribed markets; reconnecting later")
                stop.set()
                await ws.close()
                return

    def is_healthy(self) -> bool:
        if not self._connected or not self._subscribed_markets:
            return False
        now = time.monotonic()
        if self._last_pong_at:
            return now - self._last_pong_at <= PING_INTERVAL_S * 3
        return bool(self._connected_since and now - self._connected_since <= PING_INTERVAL_S * 3)

    def status(self) -> dict[str, Any]:
        now = time.monotonic()
        return {
            "connected": self._connected,
            "subscribed_markets": len(self._subscribed_markets),
            "last_message_age_s": round(now - self._last_message_at, 1) if self._last_message_at else None,
            "last_pong_age_s": round(now - self._last_pong_at, 1) if self._last_pong_at else None,
        }

    def _set_connection_state(self, connected: bool, subscribed: set[str]) -> None:
        self._connected = connected
        self._subscribed_markets = set(subscribed)
        if connected:
            self._connected_since = time.monotonic()
            self._last_message_at = 0.0
            self._last_pong_at = 0.0
        else:
            self._connected_since = 0.0

    def _set_subscribed_markets(self, subscribed: set[str]) -> None:
        self._subscribed_markets = set(subscribed)

    def _mark_message(self) -> None:
        self._last_message_at = time.monotonic()

    def _mark_pong(self) -> None:
        now = time.monotonic()
        self._last_pong_at = now
        self._last_message_at = now

    async def _handle_event(self, event: dict[str, Any]) -> None:
        event_type = str(event.get("event_type") or event.get("type") or "").lower()
        if event_type == "order":
            await self._handle_order(event)
        elif event_type == "trade":
            self._log_trade(event)
        else:
            log.warning("USER_WS unknown event_type=%s payload=%s", event_type or "?", _clip(json.dumps(event, ensure_ascii=False)))

    async def _handle_order(self, event: dict[str, Any]) -> None:
        order_id = str(event.get("id") or "")
        order_type = str(event.get("type") or "").upper()
        status = str(event.get("status") or "").upper()
        market = str(event.get("market") or "")
        side = str(event.get("side") or "")
        price = str(event.get("price") or "")
        original = str(event.get("original_size") or "")
        matched = str(event.get("size_matched") or "")
        key = (order_id, order_type, status)
        if key not in self._seen_events:
            self._seen_events.add(key)
            log.info(
                "USER_WS order type=%s status=%s id=%s market=%s side=%s price=%s original=%s matched=%s",
                order_type or "?",
                status or "?",
                _short(order_id),
                _short(market),
                side or "?",
                price or "?",
                original or "?",
                matched or "?",
            )

        if not order_id:
            return

        matched_size = _to_float(matched) or 0.0
        local_pos = await db.get_position(order_id)
        local_side = str((local_pos or {}).get("side") or side or "").upper()
        local_size = _to_float((local_pos or {}).get("size")) or _to_float(original) or 0.0
        updated = await order_manager.handle_order_update(
            order_id=order_id,
            matched_size=matched_size,
            status=status,
            order_type=order_type,
        )
        if updated is None:
            return

        effective_matched = float(updated.get("matched_size") or matched_size)
        if effective_matched > 0:
            fill_kind = "filled" if local_size > 0 and effective_matched >= local_size - 0.0001 else "partial_fill"
            log.info(
                "USER_WS %s side=%s id=%s matched=%s size=%s state=%s",
                fill_kind,
                local_side or "?",
                _short(order_id),
                effective_matched,
                local_size or "?",
                updated.get("status") or "?",
            )

    def _log_trade(self, event: dict[str, Any]) -> None:
        trade_id = str(event.get("id") or event.get("taker_order_id") or "")
        status = str(event.get("status") or "").upper()
        market = str(event.get("market") or "")
        side = str(event.get("side") or "")
        price = str(event.get("price") or "")
        size = str(event.get("size") or "")
        key = (trade_id, "TRADE", status)
        if key in self._seen_events:
            return
        self._seen_events.add(key)
        log.info(
            "USER_WS trade status=%s id=%s market=%s side=%s price=%s size=%s",
            status or "?",
            _short(trade_id),
            _short(market),
            side or "?",
            price or "?",
            size or "?",
        )

    async def _active_markets(self) -> set[str]:
        positions = await db.get_active_positions()
        return {str(p.get("condition_id") or "") for p in positions if p.get("condition_id")}


def _short(value: str, head: int = 10, tail: int = 6) -> str:
    if not value:
        return "?"
    return value if len(value) <= head + tail + 1 else f"{value[:head]}...{value[-tail:]}"


def _clip(value: str, limit: int = 500) -> str:
    return value if len(value) <= limit else value[:limit] + "..."


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
