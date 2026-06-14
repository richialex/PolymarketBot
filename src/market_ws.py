from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field
from typing import Any

import websockets

from src import db
from src.logging_setup import configure_ws_file_logger


MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
PING_INTERVAL_S = 10
REFRESH_INTERVAL_S = 5
UNSUBSCRIBE_DELAY_S = 60
FRESH_TTL_S = 90
PRICE_CHANGE_LOG_INTERVAL_S = 30

log = configure_ws_file_logger("market_ws")


@dataclass(frozen=True)
class PriceLevel:
    price: str
    size: str


@dataclass(frozen=True)
class LiveOrderBook:
    asset_id: str
    market: str = ""
    bids: list[PriceLevel] = field(default_factory=list)
    asks: list[PriceLevel] = field(default_factory=list)
    timestamp: str = ""
    hash: str = ""


@dataclass
class _BookState:
    asset_id: str
    market: str = ""
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    best_bid: float | None = None
    best_ask: float | None = None
    spread: float | None = None
    timestamp: str = ""
    hash: str = ""
    updated_at: float = 0.0
    has_snapshot: bool = False
    event_count: int = 0
    last_trade_price: float | None = None
    last_trade_side: str = ""
    tick_size: str = ""
    resolved: bool = False
    snapshot_epoch: int = 0
    last_price_change_log_at: float = 0.0
    logged_best_bid: float | None = None
    logged_best_ask: float | None = None

    def as_order_book(self) -> LiveOrderBook:
        bids = [
            PriceLevel(price=_fmt_price(price), size=_fmt_size(size))
            for price, size in sorted(self.bids.items(), reverse=True)
            if size > 0
        ]
        asks = [
            PriceLevel(price=_fmt_price(price), size=_fmt_size(size))
            for price, size in sorted(self.asks.items())
            if size > 0
        ]
        return LiveOrderBook(
            asset_id=self.asset_id,
            market=self.market,
            bids=bids,
            asks=asks,
            timestamp=self.timestamp,
            hash=self.hash,
        )


class MarketWsWatcher:
    def __init__(self, fresh_ttl_s: float = FRESH_TTL_S) -> None:
        self._fresh_ttl_s = fresh_ttl_s
        self._books: dict[str, _BookState] = {}
        self._changed_assets: asyncio.Queue[str] = asyncio.Queue(maxsize=1000)
        self._lock = asyncio.Lock()
        self._connected = False
        self._connection_epoch = 0
        self._connected_since = 0.0
        self._last_message_at = 0.0
        self._last_pong_at = 0.0
        self._subscribed_assets: set[str] = set()
        self._ws_book_hits = 0
        self._rest_book_fallbacks = 0

    async def run(self) -> None:
        backoff = 1
        while True:
            try:
                assets = await self._active_assets()
                if not assets:
                    log.info("MARKET_WS idle: no active assets")
                    await self._set_connection_state(False, set())
                    await asyncio.sleep(REFRESH_INTERVAL_S)
                    continue
                await self._run_connection(assets)
                backoff = 1
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.warning("MARKET_WS reconnect after error: %s", e)
                await self._mark_all_stale()
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 60)

    async def get_order_book(self, asset_id: str, require_fresh: bool = True) -> LiveOrderBook | None:
        async with self._lock:
            state = self._books.get(str(asset_id))
            if state is None or not state.has_snapshot or state.resolved:
                self._rest_book_fallbacks += 1
                return None
            if require_fresh and not self._is_usable_locked(state):
                self._rest_book_fallbacks += 1
                return None
            self._ws_book_hits += 1
            return state.as_order_book()

    async def get_best_bid_ask(self, asset_id: str, require_fresh: bool = True) -> tuple[float | None, float | None]:
        async with self._lock:
            state = self._books.get(str(asset_id))
            if state is None or state.resolved:
                return None, None
            if require_fresh and not self._is_usable_locked(state):
                return None, None
            return state.best_bid, state.best_ask

    async def next_changed_asset(self) -> str:
        return await self._changed_assets.get()

    async def is_fresh(self, asset_id: str) -> bool:
        async with self._lock:
            state = self._books.get(str(asset_id))
            return bool(state and self._is_usable_locked(state))

    async def status(self) -> dict[str, Any]:
        async with self._lock:
            now = time.monotonic()
            usable_books = sum(1 for s in self._books.values() if self._is_usable_locked(s))
            return {
                "connected": self._connected,
                "subscribed_assets": len(self._subscribed_assets),
                "cached_books": len(self._books),
                "usable_books": usable_books,
                "ws_book_hits": self._ws_book_hits,
                "rest_book_fallbacks": self._rest_book_fallbacks,
                "last_message_age_s": round(now - self._last_message_at, 1) if self._last_message_at else None,
                "last_pong_age_s": round(now - self._last_pong_at, 1) if self._last_pong_at else None,
            }

    async def _run_connection(self, initial_assets: set[str]) -> None:
        subscribed = set(initial_assets)
        pending_unsub: dict[str, float] = {}
        stop = asyncio.Event()

        sub = {"assets_ids": sorted(subscribed), "type": "market", "custom_feature_enabled": True}
        log.info("MARKET_WS connecting assets=%d", len(subscribed))

        async with websockets.connect(MARKET_WS_URL, ping_interval=None) as ws:
            await self._set_connection_state(True, subscribed)
            await ws.send(json.dumps(sub))
            log.info("MARKET_WS subscribed initial assets=%d", len(subscribed))

            tasks = [
                asyncio.create_task(self._heartbeat(ws, stop)),
                asyncio.create_task(self._subscription_loop(ws, stop, subscribed, pending_unsub)),
                asyncio.create_task(self._receive_loop(ws, stop)),
            ]
            try:
                await asyncio.gather(*tasks)
            finally:
                stop.set()
                await self._set_connection_state(False, set())
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)

    async def _heartbeat(self, ws, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(PING_INTERVAL_S)
            if stop.is_set():
                return
            await ws.send("PING")

    async def _subscription_loop(self, ws, stop: asyncio.Event, subscribed: set[str], pending_unsub: dict[str, float]) -> None:
        while not stop.is_set():
            await asyncio.sleep(REFRESH_INTERVAL_S)
            active = await self._active_assets()
            now = time.monotonic()

            new_assets = sorted(active - subscribed)
            if new_assets:
                await ws.send(json.dumps({
                    "assets_ids": new_assets,
                    "operation": "subscribe",
                    "custom_feature_enabled": True,
                }))
                subscribed.update(new_assets)
                await self._set_subscribed_assets(subscribed)
                for asset in new_assets:
                    pending_unsub.pop(asset, None)
                log.info("MARKET_WS subscribe assets=%d total=%d", len(new_assets), len(subscribed))

            for asset in active:
                pending_unsub.pop(asset, None)

            for asset in subscribed - active:
                pending_unsub.setdefault(asset, now + UNSUBSCRIBE_DELAY_S)

            ready = sorted(asset for asset, deadline in pending_unsub.items() if deadline <= now)
            if ready:
                await ws.send(json.dumps({"assets_ids": ready, "operation": "unsubscribe"}))
                for asset in ready:
                    subscribed.discard(asset)
                    pending_unsub.pop(asset, None)
                await self._drop_assets(ready)
                await self._set_subscribed_assets(subscribed)
                log.info("MARKET_WS unsubscribe assets=%d total=%d", len(ready), len(subscribed))

            if not subscribed:
                log.info("MARKET_WS no subscribed assets; reconnecting later")
                stop.set()
                await ws.close()
                return

    async def _receive_loop(self, ws, stop: asyncio.Event) -> None:
        while not stop.is_set():
            msg = await ws.recv()
            if msg == "PONG":
                await self._mark_pong()
                continue
            await self._mark_message()
            try:
                event = json.loads(msg)
            except json.JSONDecodeError:
                log.warning("MARKET_WS raw non-json %s", _clip(str(msg)))
                continue
            await self._handle_message(event)

    async def _handle_message(self, event: Any) -> None:
        if isinstance(event, list):
            for item in event:
                await self._handle_message(item)
            return
        if not isinstance(event, dict):
            log.warning("MARKET_WS unknown payload=%s", _clip(json.dumps(event, ensure_ascii=False)))
            return

        event_type = str(event.get("event_type") or event.get("type") or "").lower()
        if event_type == "book":
            await self._handle_book(event)
        elif event_type == "price_change":
            await self._handle_price_change(event)
        elif event_type == "best_bid_ask":
            await self._handle_best_bid_ask(event)
        elif event_type == "last_trade_price":
            await self._handle_last_trade_price(event)
        elif event_type == "tick_size_change":
            await self._handle_tick_size_change(event)
        elif event_type == "market_resolved":
            await self._handle_market_resolved(event)
        elif event_type == "new_market":
            log.info("MARKET_WS new_market market=%s", _short(str(event.get("market") or "")))
        else:
            log.warning("MARKET_WS unknown event_type=%s payload=%s", event_type or "?", _clip(json.dumps(event, ensure_ascii=False)))

    async def _handle_book(self, event: dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return
        bids = _levels_to_map(event.get("bids") or [])
        asks = _levels_to_map(event.get("asks") or [])
        async with self._lock:
            state = self._state_locked(asset_id)
            state.market = str(event.get("market") or state.market)
            state.bids = bids
            state.asks = asks
            state.timestamp = str(event.get("timestamp") or "")
            state.hash = str(event.get("hash") or "")
            state.updated_at = time.monotonic()
            state.has_snapshot = True
            state.snapshot_epoch = self._connection_epoch
            state.event_count += 1
            self._refresh_top_locked(state)
            log.info(
                "MARKET_WS book asset=%s bids=%d asks=%d best_bid=%s best_ask=%s",
                _short(asset_id),
                len(state.bids),
                len(state.asks),
                _fmt_optional(state.best_bid),
                _fmt_optional(state.best_ask),
            )
        self._notify_changed_assets({asset_id})

    async def _handle_price_change(self, event: dict[str, Any]) -> None:
        changes = event.get("price_changes") or []
        if not isinstance(changes, list):
            return
        touched: set[str] = set()
        async with self._lock:
            for change in changes:
                if not isinstance(change, dict):
                    continue
                asset_id = str(change.get("asset_id") or "")
                if not asset_id:
                    continue
                state = self._state_locked(asset_id)
                state.market = str(event.get("market") or state.market)
                state.timestamp = str(event.get("timestamp") or state.timestamp)
                state.hash = str(change.get("hash") or state.hash)
                state.updated_at = time.monotonic()
                state.event_count += 1
                self._apply_price_level_locked(state, change)
                touched.add(asset_id)

            for asset_id in touched:
                state = self._books[asset_id]
                self._refresh_top_locked(state)
                if self._should_log_price_change_locked(state):
                    state.last_price_change_log_at = time.monotonic()
                    state.logged_best_bid = state.best_bid
                    state.logged_best_ask = state.best_ask
                    log.info(
                        "MARKET_WS price_change asset=%s best_bid=%s best_ask=%s changes=%d",
                        _short(asset_id),
                        _fmt_optional(state.best_bid),
                        _fmt_optional(state.best_ask),
                        len(changes),
                    )
        self._notify_changed_assets(touched)

    async def _handle_best_bid_ask(self, event: dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return
        async with self._lock:
            state = self._state_locked(asset_id)
            state.market = str(event.get("market") or state.market)
            state.best_bid = _to_float(event.get("best_bid"))
            state.best_ask = _to_float(event.get("best_ask"))
            state.spread = _to_float(event.get("spread"))
            state.timestamp = str(event.get("timestamp") or state.timestamp)
            state.updated_at = time.monotonic()
            state.event_count += 1
            log.info(
                "MARKET_WS best_bid_ask asset=%s best_bid=%s best_ask=%s spread=%s snapshot=%s",
                _short(asset_id),
                _fmt_optional(state.best_bid),
                _fmt_optional(state.best_ask),
                _fmt_optional(state.spread),
                state.has_snapshot,
            )
        self._notify_changed_assets({asset_id})

    async def _handle_last_trade_price(self, event: dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return
        async with self._lock:
            state = self._state_locked(asset_id)
            state.market = str(event.get("market") or state.market)
            state.last_trade_price = _to_float(event.get("price"))
            state.last_trade_side = str(event.get("side") or "")
            state.timestamp = str(event.get("timestamp") or state.timestamp)
            state.updated_at = time.monotonic()
            state.event_count += 1
            log.info(
                "MARKET_WS last_trade asset=%s side=%s price=%s size=%s",
                _short(asset_id),
                state.last_trade_side or "?",
                _fmt_optional(state.last_trade_price),
                str(event.get("size") or "?"),
            )

    async def _handle_tick_size_change(self, event: dict[str, Any]) -> None:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return
        async with self._lock:
            state = self._state_locked(asset_id)
            state.market = str(event.get("market") or state.market)
            state.tick_size = str(event.get("new_tick_size") or "")
            state.timestamp = str(event.get("timestamp") or state.timestamp)
            state.updated_at = time.monotonic()
            state.event_count += 1
            log.warning(
                "MARKET_WS tick_size_change asset=%s old=%s new=%s",
                _short(asset_id),
                str(event.get("old_tick_size") or "?"),
                state.tick_size or "?",
            )

    async def _handle_market_resolved(self, event: dict[str, Any]) -> None:
        asset_ids = _event_asset_ids(event)
        async with self._lock:
            for asset_id in asset_ids:
                state = self._state_locked(asset_id)
                state.market = str(event.get("market") or state.market)
                state.resolved = True
                state.updated_at = time.monotonic()
            log.warning(
                "MARKET_WS market_resolved market=%s assets=%d winning_asset=%s",
                _short(str(event.get("market") or "")),
                len(asset_ids),
                _short(str(event.get("winning_asset_id") or "")),
            )

    async def _active_assets(self) -> set[str]:
        positions = await db.get_active_positions()
        return {str(p.get("token_id") or "") for p in positions if p.get("token_id")}

    async def _drop_assets(self, assets: list[str]) -> None:
        async with self._lock:
            for asset in assets:
                self._books.pop(asset, None)

    async def _mark_all_stale(self) -> None:
        async with self._lock:
            for state in self._books.values():
                state.updated_at = 0.0
                state.snapshot_epoch = 0

    async def _set_connection_state(self, connected: bool, subscribed: set[str]) -> None:
        async with self._lock:
            if connected:
                self._connection_epoch += 1
                self._connected_since = time.monotonic()
                self._last_message_at = 0.0
                self._last_pong_at = 0.0
            self._connected = connected
            self._subscribed_assets = set(subscribed)

    async def _set_subscribed_assets(self, subscribed: set[str]) -> None:
        async with self._lock:
            self._subscribed_assets = set(subscribed)

    async def _mark_message(self) -> None:
        async with self._lock:
            self._last_message_at = time.monotonic()

    async def _mark_pong(self) -> None:
        async with self._lock:
            now = time.monotonic()
            self._last_pong_at = now
            self._last_message_at = now

    def _state_locked(self, asset_id: str) -> _BookState:
        state = self._books.get(asset_id)
        if state is None:
            state = _BookState(asset_id=asset_id)
            self._books[asset_id] = state
        return state

    def _is_fresh_locked(self, state: _BookState) -> bool:
        return state.updated_at > 0 and time.monotonic() - state.updated_at <= self._fresh_ttl_s

    def _is_usable_locked(self, state: _BookState) -> bool:
        if not state.has_snapshot or state.resolved:
            return False
        if not self._connected:
            return False
        if state.asset_id not in self._subscribed_assets:
            return False
        return state.snapshot_epoch == self._connection_epoch

    def _apply_price_level_locked(self, state: _BookState, change: dict[str, Any]) -> None:
        price = _to_float(change.get("price"))
        size = _to_float(change.get("size"))
        side = str(change.get("side") or "").upper()
        if price is None or size is None:
            return
        levels = state.bids if side == "BUY" else state.asks if side == "SELL" else None
        if levels is None:
            return
        if size <= 0:
            levels.pop(price, None)
        else:
            levels[price] = size

    def _refresh_top_locked(self, state: _BookState) -> None:
        state.best_bid = max(state.bids) if state.bids else None
        state.best_ask = min(state.asks) if state.asks else None
        if state.best_bid is not None and state.best_ask is not None:
            state.spread = max(0.0, state.best_ask - state.best_bid)
        else:
            state.spread = None

    def _should_log_price_change_locked(self, state: _BookState) -> bool:
        best_changed = state.best_bid != state.logged_best_bid or state.best_ask != state.logged_best_ask
        interval_elapsed = time.monotonic() - state.last_price_change_log_at >= PRICE_CHANGE_LOG_INTERVAL_S
        return best_changed or interval_elapsed

    def _notify_changed_assets(self, asset_ids: set[str]) -> None:
        for asset_id in asset_ids:
            if not asset_id:
                continue
            try:
                self._changed_assets.put_nowait(asset_id)
            except asyncio.QueueFull:
                # The bot still has the latest book cached; dropping an old wake-up
                # is better than letting WS receive processing back up.
                try:
                    self._changed_assets.get_nowait()
                except asyncio.QueueEmpty:
                    pass
                try:
                    self._changed_assets.put_nowait(asset_id)
                except asyncio.QueueFull:
                    pass


def _levels_to_map(levels: list[Any]) -> dict[float, float]:
    parsed: dict[float, float] = {}
    for level in levels:
        price = _to_float(level.get("price") if isinstance(level, dict) else getattr(level, "price", None))
        size = _to_float(level.get("size") if isinstance(level, dict) else getattr(level, "size", None))
        if price is not None and size is not None and size > 0:
            parsed[price] = size
    return parsed


def _event_asset_ids(event: dict[str, Any]) -> list[str]:
    values = event.get("asset_ids") or event.get("assets_ids") or []
    if isinstance(values, str):
        values = [values]
    winning = str(event.get("winning_asset_id") or "")
    if winning:
        values = [*values, winning]
    return sorted({str(v) for v in values if v})


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _fmt_price(value: float) -> str:
    return f"{value:.3f}".rstrip("0").rstrip(".")


def _fmt_size(value: float) -> str:
    return f"{value:.6f}".rstrip("0").rstrip(".")


def _fmt_optional(value: float | None) -> str:
    return "?" if value is None else _fmt_price(value)


def _short(value: str, head: int = 10, tail: int = 6) -> str:
    if not value:
        return "?"
    return value if len(value) <= head + tail + 1 else f"{value[:head]}...{value[-tail:]}"


def _clip(value: str, limit: int = 500) -> str:
    return value if len(value) <= limit else value[:limit] + "..."
