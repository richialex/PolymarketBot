"""Main farming loop."""
from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from src import db
from src.pm_client import client
from src.scanner import (
    enrich_batch, scan_markets,
    calc_order_price, calc_sell_order_price,
    mid_from_order_book,
    _extract_bids, _extract_asks, _ob_spread_cents, bid_depth_spread_cents,
    multi_reward_from_api, ScoredMarket,
)
from src.market_ws import MarketWsWatcher
from src.user_ws import UserWsWatcher

log = logging.getLogger(__name__)


SCAN_BATCH_SIZE  = 100   # markets enriched per tick
REWARDS_CACHE_TTL = 1800  # seconds before refreshing full rewards list
POOL_MAX_SIZE    = 50    # best candidates kept in memory across ticks
ACTIVE_REWARD_CHECK_INTERVAL_S = 120
ACTIVE_REWARD_CHECK_DELAY_S = 0.25


MONITOR_INTERVAL = 5   # seconds between position checks
TRADE_INTERVAL = 10    # seconds between trade decisions when scanner runs separately
ORDER_STATUS_REST_INTERVAL_S = 30
ORDER_STATUS_REST_HEALTHY_WS_INTERVAL_S = 300
AUTO_RECONCILE_INTERVAL_S = 20
FAST_STEP_DEBOUNCE_S = 0.75
FAST_STEP_COOLDOWN_S = 6.0


class FarmingBot:
    def __init__(self) -> None:
        self.running = False
        self._scan_task: asyncio.Task | None = None
        self._trader_task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None
        self._fast_step_task: asyncio.Task | None = None
        self._user_ws_task: asyncio.Task | None = None
        self._market_ws_task: asyncio.Task | None = None
        self._user_ws = UserWsWatcher()
        self._market_ws = MarketWsWatcher()
        self.last_scan: list[ScoredMarket] = []
        self.shown_markets: list[ScoredMarket] = []
        self.errors: list[str] = []
        self._last_depth: str | None = None
        self._scan_lock = asyncio.Lock()
        self._last_order_status_check: dict[str, float] = {}
        self._last_reconcile_at: float = 0.0
        self._level_share_breach_since: dict[str, float] = {}
        self._fast_step_cooldown_until: dict[str, float] = {}
        self._replacing_condition_ids: set[str] = set()

        # Front-run protection state
        self._fr_prev_size: dict[str, float] = {}        # token → previous best bid size (shares)
        self._fr_prev_time: dict[str, float] = {}        # token → previous timestamp
        self._fr_cooldown_until: dict[str, float] = {}   # token → cooldown expiry
        self._last_active_reward_check_at: float = 0.0

        # Rotating scanner state
        self._rewards_cache: list = []
        self._rewards_cache_time: datetime | None = None
        self._rewards_cache_min_daily: float | None = None  # min_daily used when cache was built
        self._rewards_cache_scanner_mode: str | None = None
        self._scan_cursor: int = 0
        self._candidates_pool: dict[str, ScoredMarket] = {}  # best seen across all ticks
        self.scan_status: dict = {
            "rewards_total": 0,
            "rewards_passing": 0,
            "batch_size": 0,
            "batch_scored": 0,
            "pool_size": 0,
            "shown": 0,
            "cursor": 0,
            "last_updated": None,
            "scanner_mode": "legacy",
        }

    # ── Public API ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._scan_task = asyncio.create_task(self._scanner_loop())
        self._trader_task = asyncio.create_task(self._trader_loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        self._fast_step_task = asyncio.create_task(self._fast_step_loop())
        self._user_ws_task = asyncio.create_task(self._user_ws.run())
        self._market_ws_task = asyncio.create_task(self._market_ws.run())
        log.info("FarmingBot started")

    def stop(self) -> None:
        self.running = False
        for t in (
            self._scan_task,
            self._trader_task,
            self._monitor_task,
            self._fast_step_task,
            self._user_ws_task,
            self._market_ws_task,
        ):
            if t:
                t.cancel()
        self._scan_task = None
        self._trader_task = None
        self._monitor_task = None
        self._fast_step_task = None
        self._user_ws_task = None
        self._market_ws_task = None
        log.info("FarmingBot stopped")

    def forget_market(self, condition_id: str) -> None:
        """Drop a market from in-memory scanner state immediately."""
        self._candidates_pool.pop(condition_id, None)
        self.last_scan = [m for m in self.last_scan if m.condition_id != condition_id]
        self.shown_markets = [m for m in self.shown_markets if m.condition_id != condition_id]

    async def reconcile(self) -> dict:
        """Reconcile local order journal with Polymarket open orders and holdings."""
        local_positions = await db.get_reconcilable_positions()
        open_orders = await client.list_open_orders()
        open_by_id = {str(getattr(o, "id", "") or ""): o for o in open_orders}
        adopted_pending = 0
        marked_filled = 0
        marked_cancelled = 0
        marked_unknown = 0

        for pos in local_positions:
            order_id = pos["order_id"]
            status = str(pos.get("status", "") or "").upper()

            if status == "PENDING_PLACE":
                match = self._find_matching_open_order(pos, open_orders)
                if match is not None:
                    real_id = str(getattr(match, "id", "") or "")
                    await db.replace_position_order_id(order_id, real_id, status="OPEN")
                    adopted_pending += 1
                    continue

                if await self._has_token_position(pos):
                    await db.update_position_status(
                        order_id,
                        "FILLED",
                        filled_at=datetime.now(timezone.utc).isoformat(),
                    )
                    marked_filled += 1
                else:
                    await db.update_position_status(order_id, "UNKNOWN")
                    marked_unknown += 1
                continue

            if order_id in open_by_id:
                continue

            order = await client.get_order(order_id)
            remote_status = str(getattr(order, "status", "") or "").upper() if order else ""
            if remote_status in ("FILLED", "MATCHED"):
                await db.update_position_status(
                    order_id,
                    "FILLED",
                    filled_at=datetime.now(timezone.utc).isoformat(),
                )
                marked_filled += 1
            elif remote_status in ("CANCELLED", "CANCELED"):
                await db.update_position_status(order_id, "CANCELLED")
                marked_cancelled += 1
            elif pos.get("side") == "BUY" and await self._has_token_position(pos):
                await db.update_position_status(
                    order_id,
                    "FILLED",
                    filled_at=datetime.now(timezone.utc).isoformat(),
                )
                marked_filled += 1
            elif status not in ("SELL_PENDING",):
                await db.update_position_status(order_id, "UNKNOWN")
                marked_unknown += 1

        await self._recover_missing_sells()
        self._last_reconcile_at = time.monotonic()
        return {
            "checked": len(local_positions),
            "open_orders": len(open_orders),
            "adopted_pending": adopted_pending,
            "marked_filled": marked_filled,
            "marked_cancelled": marked_cancelled,
            "marked_unknown": marked_unknown,
        }

    def _find_matching_open_order(self, pos: dict, open_orders: list) -> object | None:
        token_id = str(pos.get("token_id", "") or "")
        side = str(pos.get("side", "") or "").upper()
        price = float(pos.get("price", 0) or 0)
        size = float(pos.get("size", 0) or 0)
        for order in open_orders:
            try:
                if str(getattr(order, "token_id", "") or "") != token_id:
                    continue
                if str(getattr(order, "side", "") or "").upper() != side:
                    continue
                if abs(float(getattr(order, "price", 0) or 0) - price) > 0.0001:
                    continue
                original = float(getattr(order, "original_size", 0) or 0)
                matched = float(getattr(order, "size_matched", 0) or 0)
                remaining = max(0.0, original - matched)
                if abs(original - size) <= 0.01 or abs(remaining - size) <= 0.01:
                    return order
            except (TypeError, ValueError):
                continue
        return None

    async def _has_token_position(self, pos: dict) -> bool:
        token_id = str(pos.get("token_id", "") or "")
        expected = float(pos.get("size", 0) or 0)
        actual = await client.get_token_position_size(token_id)
        return actual >= max(0.0001, min(expected, 0.0001))

    async def _get_order_book_ws_first(self, token_id: str, purpose: str):
        order_book = await self._market_ws.get_order_book(token_id)
        if order_book is not None:
            return order_book
        log.info("MARKET_WS fallback_rest purpose=%s token=%s", purpose, _short_id(token_id))
        return await client.get_order_book(token_id)

    async def ws_status(self) -> dict:
        return {
            "market_ws": await self._market_ws.status(),
            "user_ws": self._user_ws.status(),
        }

    def _should_check_order_status(self, order_id: str, interval_s: int = ORDER_STATUS_REST_INTERVAL_S) -> bool:
        now = time.monotonic()
        last = self._last_order_status_check.get(order_id, 0.0)
        if now - last < interval_s:
            return False
        self._last_order_status_check[order_id] = now
        return True

    # ── Main loop ───────────────────────────────────────────────────────────────

    async def _scanner_loop(self) -> None:
        while self.running:
            try:
                cfg = await self._load_cfg()
                await self.scan_once(cfg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                msg = f"{datetime.now(timezone.utc).isoformat()} {e}"
                self.errors = ([msg] + self.errors)[:50]
                log.exception("Scanner loop error: %s", e)

            interval = await db.get_setting("scan_interval_s", 60)
            await asyncio.sleep(int(interval))

    async def _trader_loop(self) -> None:
        await asyncio.sleep(2)
        while self.running:
            try:
                cfg = await self._load_cfg()
                await self.trade_once(cfg)
                monitor_interval = int(cfg.get("monitor_interval_s", MONITOR_INTERVAL))
                interval = max(MONITOR_INTERVAL, min(TRADE_INTERVAL, monitor_interval))
            except asyncio.CancelledError:
                break
            except Exception as e:
                msg = f"{datetime.now(timezone.utc).isoformat()} {e}"
                self.errors = ([msg] + self.errors)[:50]
                log.exception("Trader loop error: %s", e)
                interval = MONITOR_INTERVAL
            await asyncio.sleep(max(MONITOR_INTERVAL, interval))

    async def _monitor_loop(self) -> None:
        """Fast loop: check open positions every monitor_interval_s seconds.
        Handles fills, cancellations, and price drift independently of the main scan."""
        await asyncio.sleep(MONITOR_INTERVAL)  # let main loop init first
        while self.running:
            try:
                cfg = await self._load_cfg()
                # The user WS can stay connected while dropping an individual
                # order update. Reconcile is the authoritative fallback: it
                # removes locally OPEN orders that no longer exist remotely.
                if time.monotonic() - self._last_reconcile_at >= AUTO_RECONCILE_INTERVAL_S:
                    result = await self.reconcile()
                    if result["marked_filled"] or result["marked_cancelled"] or result["marked_unknown"]:
                        log.info("Auto reconcile: %s", result)
                await self._monitor_positions(cfg)
                interval = int(cfg.get("monitor_interval_s", MONITOR_INTERVAL))
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Monitor loop error: %s", e)
                interval = MONITOR_INTERVAL
            await asyncio.sleep(interval)

    async def _fast_step_loop(self) -> None:
        """React quickly to WS book changes that make an open BUY the best bid.

        This is intentionally narrower than the full monitor loop: it only steps
        BUY orders down in mid/edge depth modes. Size changes, top-ups, exits, and SELL
        handling stay in the regular monitor loop to avoid over-trading on noisy
        book flicker.
        """
        pending: dict[str, float] = {}
        while self.running:
            try:
                now = time.monotonic()
                timeout = None
                if pending:
                    timeout = max(0.0, min(pending.values()) - now)

                try:
                    asset_id = await asyncio.wait_for(
                        self._market_ws.next_changed_asset(),
                        timeout=timeout,
                    )
                    cooldown_until = self._fast_step_cooldown_until.get(asset_id, 0.0)
                    if time.monotonic() >= cooldown_until:
                        pending[asset_id] = time.monotonic() + FAST_STEP_DEBOUNCE_S
                except asyncio.TimeoutError:
                    pass

                now = time.monotonic()
                due = [asset for asset, deadline in pending.items() if deadline <= now]
                for asset_id in due:
                    pending.pop(asset_id, None)
                    await self._check_front_run(asset_id)
                    await self._fast_step_down_asset(asset_id)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Fast step loop error: %s", e)
                await asyncio.sleep(1)

    async def _check_front_run(self, token_id: str) -> None:
        """Cancel BUY if thin level ahead is being eaten rapidly."""
        cfg = await self._load_cfg()
        if not cfg.get("front_run_protection", True):
            return

        now = time.monotonic()
        if now < self._fr_cooldown_until.get(token_id, 0.0):
            return

        positions = await db.get_open_positions()
        pos = next(
            (
                p for p in positions
                if str(p.get("token_id") or "") == str(token_id)
                and str(p.get("side") or "").upper() == "BUY"
            ),
            None,
        )
        if pos is None:
            self._fr_prev_size.pop(token_id, None)
            self._fr_prev_time.pop(token_id, None)
            return

        order_book = await self._market_ws.get_order_book(token_id)
        if order_book is None:
            return

        current_price = float(pos.get("price", 0) or 0)
        live_bids = _extract_bids(order_book)
        if not live_bids:
            return

        levels = sorted(set(round(b, 3) for b in live_bids), reverse=True)
        best_bid = levels[0]

        # If we ARE the best bid, the existing step-down logic handles it
        if abs(current_price - best_bid) < 0.0005:
            return

        # Get best bid size in USD
        best_bid_usd = 0.0
        for level in (getattr(order_book, "bids", None) or []):
            try:
                lv_price = float(getattr(level, "price", 0) or 0)
                lv_size = float(getattr(level, "size", 0) or 0)
            except (TypeError, ValueError):
                continue
            if abs(lv_price - best_bid) <= 0.0005:
                best_bid_usd += lv_price * lv_size

        threshold = float(cfg.get("front_run_bid_threshold_usd", 15.0) or 15.0)
        if best_bid_usd >= threshold:
            # Level ahead is thick — no protection needed, reset tracking
            self._fr_prev_size.pop(token_id, None)
            self._fr_prev_time.pop(token_id, None)
            return

        # Track consumption
        prev_size = self._fr_prev_size.get(token_id)
        prev_time = self._fr_prev_time.get(token_id)
        self._fr_prev_size[token_id] = best_bid_usd
        self._fr_prev_time[token_id] = now

        if prev_size is None or prev_time is None or prev_size <= 0:
            return

        elapsed = now - prev_time
        if elapsed <= 0:
            return

        consumed = prev_size - best_bid_usd
        if consumed <= 0:
            return

        eat_pct = float(cfg.get("front_run_eat_pct", 30.0) or 30.0)
        window_s = float(cfg.get("front_run_window_s", 3.0) or 3.0)
        rate_per_s = (consumed / prev_size) / elapsed
        threshold_rate = (eat_pct / 100.0) / window_s

        if rate_per_s >= threshold_rate:
            cooldown_s = float(cfg.get("front_run_cooldown_s", 15.0) or 15.0)
            log.info(
                "FRONT_RUN Cancel BUY %s: best bid $%.2f being eaten (%.0f%%/s ≥ %.0f%%/%.0fs threshold) | %s",
                pos["order_id"], best_bid_usd, rate_per_s * 100, eat_pct, window_s,
                pos.get("market_question", "")[:45],
            )
            cancelled = await client.cancel_order(pos["order_id"])
            if cancelled:
                await db.update_position_status(pos["order_id"], "CANCELLED")
                self._candidates_pool.pop(pos.get("condition_id"), None)
            self._fr_cooldown_until[token_id] = time.monotonic() + cooldown_s
            self._fr_prev_size.pop(token_id, None)
            self._fr_prev_time.pop(token_id, None)

    async def _fast_step_down_asset(self, token_id: str) -> None:
        cfg = await self._load_cfg()
        depth = str(cfg.get("depth") or "").lower()
        if depth == "first":
            return

        now = time.monotonic()
        if now < self._fast_step_cooldown_until.get(token_id, 0.0):
            return

        positions = await db.get_open_positions()
        pos = next(
            (
                p for p in positions
                if str(p.get("token_id") or "") == str(token_id)
                and str(p.get("side") or "").upper() == "BUY"
            ),
            None,
        )
        if pos is None:
            self._fast_step_cooldown_until.pop(token_id, None)
            return

        condition_id = str(pos.get("condition_id") or "")
        if condition_id in self._replacing_condition_ids:
            return

        order_book = await self._market_ws.get_order_book(token_id)
        if order_book is None:
            return

        current_price = float(pos.get("price", 0) or 0)
        live_bids = _extract_bids(order_book)
        if len(live_bids) < 2:
            return

        levels = sorted(set(round(b, 3) for b in live_bids), reverse=True)
        if len(levels) < 2:
            return

        best_bid = levels[0]
        if abs(current_price - best_bid) >= 0.0005:
            return

        live_mid = mid_from_order_book(order_book)
        market_info = self._candidates_pool.get(condition_id)
        if market_info is None:
            market_info = next((m for m in self.last_scan if m.condition_id == condition_id), None)
        spread = market_info.rewards_max_spread if market_info else 0.04

        second_level = levels[1]
        lo = max(0.001, (live_mid or current_price) - spread)
        if not (lo <= second_level < current_price):
            return

        log.info(
            "FAST_STEP BUY %s front-of-queue %.2f¢ → %.2f¢ | %s",
            pos["order_id"], current_price * 100, second_level * 100,
            pos.get("market_question", "")[:45],
        )
        self._fast_step_cooldown_until[token_id] = time.monotonic() + FAST_STEP_COOLDOWN_S
        replaced = await self._replace_open_order(pos, second_level, float(pos["size"]))
        if not replaced:
            self._fast_step_cooldown_until[token_id] = time.monotonic() + 1.0

    async def tick(self, cfg: dict | None = None) -> None:
        if cfg is None:
            cfg = await self._load_cfg()
        await self.scan_once(cfg)
        await self.trade_once(cfg)

    async def scan_once(self, cfg: dict | None = None) -> list[ScoredMarket]:
        if cfg is None:
            cfg = await self._load_cfg()

        async with self._scan_lock:
            trade_candidates, shown_candidates = await self._rotating_scan(cfg)
            self.last_scan = trade_candidates
            self.shown_markets = shown_candidates
        log.info(
            "Pool has %d candidates after scan; UI shows %d",
            len(trade_candidates), len(shown_candidates),
        )
        return shown_candidates

    async def trade_once(self, cfg: dict | None = None) -> None:
        if cfg is None:
            cfg = await self._load_cfg()

        await self._check_active_position_rewards(cfg)

        candidates = list(self.last_scan)
        if not candidates:
            log.info("Trade loop skipped: no scanned candidates yet")
            return

        # 1. Exit stale positions (expiring soon, dropped from candidates, or reward dropped)
        await self._exit_stale_positions(candidates, cfg)

        balance = await client.get_balance()
        free_balance = float(balance)
        await db.record_balance(free_balance)

        order_usdc = max(0.0, float(cfg["order_usdc"] or 0))
        capital_limit = max(0.0, float(cfg["bot_capital_limit_usdc"] or 0))
        buffer_pct = min(100.0, max(0.0, float(cfg.get("free_balance_buffer_pct", 10.0) or 0)))
        free_order_cap = max(0.0, free_balance * (1.0 - buffer_pct / 100.0))
        bot_capital_in_use = await db.get_bot_capital_in_use()
        capital_remaining = max(0.0, capital_limit - bot_capital_in_use)
        slots_remaining = math.floor(capital_remaining / order_usdc) if order_usdc > 0 else 0
        max_slots_per_market = cfg["max_slots_per_market"]

        log.info(
            "Capital: free=$%.2f usable_free=$%.2f bot_used=$%.2f limit=$%.2f | order=$%.2f remaining_slots=%d",
            free_balance, free_order_cap, bot_capital_in_use, capital_limit, order_usdc, slots_remaining,
        )

        # 2. Rebalance: cancel worst positions to make room for significantly better ones
        await self._rebalance_risk(candidates, slots_remaining, order_usdc, max_slots_per_market)

        # 3. Re-fetch and enter new markets
        active_positions = await db.get_active_positions()
        open_condition_ids = {p["condition_id"] for p in active_positions} | set(self._replacing_condition_ids)
        banned_conditions = set((await db.get_active_market_bans()).keys())
        if banned_conditions:
            self._candidates_pool = {
                cid: m for cid, m in self._candidates_pool.items()
                if cid not in banned_conditions
            }

        # Recompute after potential rebalance
        bot_capital_in_use = await db.get_bot_capital_in_use()
        capital_remaining = max(0.0, capital_limit - bot_capital_in_use)
        slots_remaining = math.floor(capital_remaining / order_usdc) if order_usdc > 0 else 0

        # Average score for super-deal detection
        avg_score = (sum(m.score for m in candidates) / len(candidates)) if candidates else 0

        log.info(
            "Enter loop: %d candidates, %d capital slots remaining, avg_score=%.4f",
            len(candidates), slots_remaining, avg_score,
        )

        for market in candidates:
            if order_usdc <= 0:
                log.info("Skip entries: order_usdc must be > 0")
                break
            if slots_remaining <= 0:
                break
            if market.condition_id in open_condition_ids:
                continue
            if market.condition_id in banned_conditions:
                self._candidates_pool.pop(market.condition_id, None)
                continue

            ratio = market.score / avg_score if avg_score > 0 else 1.0
            market_budget = order_usdc
            if market_budget < market.min_order_cost:
                log.info(
                    "Skip '%s': budget $%.2f < min_cost $%.2f",
                    market.question[:35], market_budget, market.min_order_cost,
                )
                continue
            if market_budget > free_order_cap + 0.01:
                log.info(
                    "Skip '%s': order $%.2f > usable free balance $%.2f",
                    market.question[:35], market_budget, free_order_cap,
                )
                break
            if market_budget > capital_remaining + 0.01:
                log.info(
                    "Skip '%s': order $%.2f > bot capital remaining $%.2f",
                    market.question[:35], market_budget, capital_remaining,
                )
                break

            log.info(
                "Entering '%s' budget=$%.2f score=%.4f (%.1f× avg)",
                market.question[:40], market_budget, market.score, ratio,
            )
            spent = await self._enter_market(market, market_budget, cfg)
            if spent > 0:
                free_order_cap = max(0.0, free_order_cap - spent)
                capital_remaining = max(0.0, capital_remaining - spent)
                slots_remaining = math.floor(capital_remaining / order_usdc) if order_usdc > 0 else 0
                open_condition_ids.add(market.condition_id)

    # ── Rotating scanner ───────────────────────────────────────────────────────

    async def _rotating_scan(self, cfg: dict) -> tuple[list[ScoredMarket], list[ScoredMarket]]:
        """Cycle through ALL reward markets in batches, accumulating the best in a pool."""
        now = datetime.now(timezone.utc)
        min_daily = cfg["min_daily_reward"]
        scanner_mode = str(cfg.get("scanner_mode") or "legacy").lower()
        if scanner_mode not in ("legacy", "multi"):
            scanner_mode = "legacy"
        mode_changed = self._rewards_cache_scanner_mode not in (None, scanner_mode)
        if mode_changed:
            log.info("scanner_mode changed %s -> %s, resetting rewards cache and pool", self._rewards_cache_scanner_mode, scanner_mode)
            self._rewards_cache.clear()
            self._candidates_pool.clear()
            self._scan_cursor = 0

        # Refresh the full rewards list every REWARDS_CACHE_TTL seconds
        cache_age = (now - self._rewards_cache_time).total_seconds() if self._rewards_cache_time else 9999
        min_daily_changed = self._rewards_cache_min_daily != min_daily
        if not self._rewards_cache or cache_age > REWARDS_CACHE_TTL or min_daily_changed:
            if min_daily_changed and self._rewards_cache:
                log.info("min_daily_reward changed %.1f → %.1f, forcing cache refresh + pool reset", self._rewards_cache_min_daily, min_daily)
                self._candidates_pool.clear()
            from src.pm_client import client as _c
            if scanner_mode == "multi":
                raw_rewards = await _c.get_reward_markets_multi()
                all_rewards = [m for m in (multi_reward_from_api(item) for item in raw_rewards) if m is not None]
            else:
                all_rewards = await _c.get_all_rewards()
            self._rewards_cache = [
                r for r in all_rewards
                if float(getattr(r, "total_daily_rate", 0) or 0) >= min_daily
            ]
            before_pool = len(self._candidates_pool)
            pruned_pool = 0
            if all_rewards:
                passing_ids = {str(getattr(r, "condition_id", "") or "") for r in self._rewards_cache}
                self._candidates_pool = {
                    cid: m for cid, m in self._candidates_pool.items()
                    if cid in passing_ids
                }
                self.last_scan = [m for m in self.last_scan if m.condition_id in passing_ids]
                self.shown_markets = [m for m in self.shown_markets if m.condition_id in passing_ids]
                pruned_pool = before_pool - len(self._candidates_pool)
            else:
                log.warning("Rewards refresh returned no markets; preserving existing candidate pool")
            self._rewards_cache_time = now
            self._rewards_cache_min_daily = min_daily
            self._rewards_cache_scanner_mode = scanner_mode
            self._scan_cursor = 0
            self.scan_status.update({
                "rewards_total": len(all_rewards),
                "rewards_passing": len(self._rewards_cache),
                "last_updated": now.isoformat(),
                "scanner_mode": scanner_mode,
            })
            log.info("Rewards cache refreshed mode=%s: %d markets pass min_daily>=%.1f", scanner_mode, len(self._rewards_cache), min_daily)
            if pruned_pool:
                log.info("Pruned %d stale candidate(s) below min_daily from pool", pruned_pool)

        total = len(self._rewards_cache)
        if total == 0:
            self.scan_status.update({
                "batch_size": 0,
                "batch_scored": 0,
                "pool_size": len(self._candidates_pool),
                "shown": len(self._candidates_pool),
                "cursor": self._scan_cursor,
                "last_updated": now.isoformat(),
                "scanner_mode": scanner_mode,
            })
            candidates = sorted(self._candidates_pool.values(), key=lambda m: m.score, reverse=True)
            return candidates, candidates

        # Take next batch from cursor (wrap around)
        end = min(self._scan_cursor + SCAN_BATCH_SIZE, total)
        batch = self._rewards_cache[self._scan_cursor:end]
        prev_cursor = self._scan_cursor
        self._scan_cursor = end % total if end < total else 0
        rotation_pct = end / total * 100
        log.info(
            "Rotating scan: cursor %d→%d / %d (%.0f%% of full rotation)",
            prev_cursor, end, total, rotation_pct,
        )

        # Enrich this batch
        new_markets = await enrich_batch(
            batch,
            category_blacklist=cfg["category_blacklist"],
            volatility_threshold=cfg["volatility_threshold"],
            min_spread=cfg["min_spread"],
            max_markets=SCAN_BATCH_SIZE,
            max_ob_spread=cfg["max_ob_spread"],
            max_daily_trades=cfg["max_daily_trades"],
            max_bid_depth_spread=cfg.get("max_bid_depth_spread", 4.0),
            market_rewards=batch if scanner_mode == "multi" else None,
        )

        # Merge into pool: update existing + add new
        banned_conditions = set((await db.get_active_market_bans()).keys())
        for m in new_markets:
            if m.condition_id in banned_conditions:
                continue
            self._candidates_pool[m.condition_id] = m

        if banned_conditions:
            self._candidates_pool = {
                cid: m for cid, m in self._candidates_pool.items()
                if cid not in banned_conditions
            }

        # Apply user word blacklist — purge matching markets from pool in real-time
        word_bl = [w.strip().lower() for w in cfg.get("word_blacklist", []) if w.strip()]
        if word_bl:
            self._candidates_pool = {
                cid: m for cid, m in self._candidates_pool.items()
                if not any(w in m.question.lower() for w in word_bl)
            }

        # Filter by fixed order budget: exclude markets that can never be opened.
        order_usdc = max(0.0, float(cfg.get("order_usdc", 0) or 0))
        if order_usdc > 0:
            self._candidates_pool = {
                cid: m for cid, m in self._candidates_pool.items()
                if m.min_order_cost <= order_usdc
            }

        # Prune pool: keep top POOL_MAX_SIZE by score
        pool_sorted = sorted(self._candidates_pool.values(), key=lambda m: m.score, reverse=True)
        self._candidates_pool = {m.condition_id: m for m in pool_sorted[:POOL_MAX_SIZE]}

        capital_limit = max(0.0, float(cfg.get("bot_capital_limit_usdc", 0) or 0))
        max_slots = max(1, math.floor(capital_limit / order_usdc)) if order_usdc > 0 else 1
        shown = pool_sorted[:max_slots]
        self.scan_status.update({
            "batch_size": len(batch),
            "batch_scored": len(new_markets),
            "pool_size": len(self._candidates_pool),
            "shown": len(shown),
            "cursor": self._scan_cursor,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "scanner_mode": scanner_mode,
        })
        return pool_sorted[:POOL_MAX_SIZE], shown

    # ── Risk rebalancing ───────────────────────────────────────────────────────

    async def _rebalance_risk(
        self,
        candidates: list[ScoredMarket],
        slots_remaining: int,
        slot_value: float,
        max_slots_per_market: int,
    ) -> None:
        """Cancel worst positions to free slots for a significantly better unentered market.

        Triggers when:
          1. No slots remaining for a better unentered market
          2. Best unentered score is ≥ 1.3× the worst entered score
        """
        if not candidates or slots_remaining > 0:
            return

        open_positions = await db.get_open_positions()
        if not open_positions:
            return

        active_positions = await db.get_active_positions()
        entered_ids = {p["condition_id"] for p in active_positions} | set(self._replacing_condition_ids)
        candidate_map = {m.condition_id: m for m in candidates}

        unentered = [m for m in candidates if m.condition_id not in entered_ids]
        if not unentered:
            return
        best = unentered[0]

        scorable = [p for p in open_positions
                    if p["condition_id"] in candidate_map and p.get("side") != "SELL"]
        if not scorable:
            return

        scorable.sort(key=lambda p: candidate_map[p["condition_id"]].score)
        worst_score = candidate_map[scorable[0]["condition_id"]].score

        if best.score < worst_score * 1.3:
            log.debug(
                "Rebalance skipped: best %.4f < 1.3× worst %.4f",
                best.score, worst_score,
            )
            return

        # Cancel just enough worst positions (≤ max_slots_per_market) to free 1 slot
        to_cancel = []
        for pos in scorable:
            if candidate_map[pos["condition_id"]].score >= best.score / 1.3:
                break
            to_cancel.append(pos)
            if len(to_cancel) >= max_slots_per_market:
                break

        log.info(
            "Rebalance: cancel %d position(s) → '%s' (score=%.4f, %.2f× better than worst)",
            len(to_cancel), best.question[:40], best.score,
            best.score / max(worst_score, 1e-9),
        )
        for pos in to_cancel:
            cancelled = await client.cancel_order(pos["order_id"])
            if cancelled:
                await db.update_position_status(pos["order_id"], "CANCELLED")

    # ── Exit stale positions ────────────────────────────────────────────────────

    async def _exit_stale_positions(self, candidates: list[ScoredMarket], cfg: dict | None = None) -> None:
        """Cancel open positions for markets that are expiring soon or dropped from candidates."""
        open_positions = await db.get_open_positions()
        if not open_positions:
            return

        candidate_ids = {m.condition_id for m in candidates}
        now = datetime.now(timezone.utc)
        expiry_cutoff = now + timedelta(days=2)

        # Build lookups from current candidates
        candidate_map   = {m.condition_id: m for m in candidates}
        expiry_map      = {m.condition_id: m.end_date for m in candidates}

        # Build a fast lookup: condition_id → current reward rate from rewards cache
        rewards_by_cid = {
            r.condition_id: float(getattr(r, "total_daily_rate", 0) or 0)
            for r in self._rewards_cache
        }
        min_daily      = (cfg or {}).get("min_daily_reward", 7.0)
        vol_threshold  = float((cfg or {}).get("volatility_threshold", 0.03))
        max_trades     = int((cfg or {}).get("max_daily_trades", 3))
        user_wl        = [w.strip().lower() for w in (cfg or {}).get("word_blacklist", []) if w.strip()]

        for pos in open_positions:
            cid = pos["condition_id"]
            order_id = pos["order_id"]
            reason = None

            # Check user word blacklist first
            if user_wl:
                q = pos.get("market_question", "").lower()
                hit = next((w for w in user_wl if w in q), None)
                if hit:
                    reason = f"word blacklist: '{hit}'"
            # Check if reward rate dropped below minimum since we entered
            if not reason:
                current_rate = rewards_by_cid.get(cid)
                if current_rate is not None and current_rate < min_daily:
                    reason = f"reward dropped to ${current_rate:.2f}/day (min ${min_daily:.2f})"
            # Check volatility and trade count against current thresholds
            if not reason and cid in candidate_map:
                m = candidate_map[cid]
                if m.price_volatility > vol_threshold:
                    reason = f"volatility {m.price_volatility:.4f} > {vol_threshold:.3f}"
                elif m.trade_count > max_trades:
                    reason = f"trade_count {m.trade_count} > max {max_trades}"
            # Check if market dropped out of candidates
            if not reason and cid not in candidate_ids:
                reason = "dropped from candidates"
            if not reason:
                # Check expiry for markets still in candidates
                end_date_str = expiry_map.get(cid)
                if end_date_str:
                    try:
                        end_dt = datetime.fromisoformat(end_date_str)
                        if end_dt.tzinfo is None:
                            end_dt = end_dt.replace(tzinfo=timezone.utc)
                        if end_dt < expiry_cutoff:
                            reason = f"expires soon ({end_dt.date()})"
                    except (ValueError, AttributeError):
                        pass

            if reason:
                log.info("Cancelling order %s — %s | %s", order_id, reason, pos.get("market_question", "")[:50])
                cancelled = await client.cancel_order(order_id)
                if cancelled:
                    await db.update_position_status(order_id, "CANCELLED")

    async def _check_active_position_rewards(self, cfg: dict) -> None:
        """Poll reward rates for open BUY orders and exit markets below min_daily."""
        now = time.monotonic()
        if now - self._last_active_reward_check_at < ACTIVE_REWARD_CHECK_INTERVAL_S:
            return
        self._last_active_reward_check_at = now

        min_daily = float(cfg.get("min_daily_reward", 7.0) or 0)
        if min_daily <= 0:
            return

        open_positions = await db.get_open_positions()
        positions_by_cid: dict[str, list[dict]] = {}
        for pos in open_positions:
            if str(pos.get("side") or "").upper() == "SELL":
                continue
            cid = str(pos.get("condition_id") or "")
            if cid:
                positions_by_cid.setdefault(cid, []).append(pos)
        if not positions_by_cid:
            return

        for index, (cid, positions) in enumerate(positions_by_cid.items()):
            if index:
                await asyncio.sleep(ACTIVE_REWARD_CHECK_DELAY_S)

            market_reward = await client.get_market_reward(cid)
            if market_reward is None:
                log.warning(
                    "Active reward check skipped %s: reward API returned no data | %s",
                    _short_id(cid), positions[0].get("market_question", "")[:50],
                )
                continue

            current_rate = self._daily_reward_rate(market_reward)
            if current_rate is None:
                log.warning(
                    "Active reward check skipped %s: no rate_per_day in reward config | %s",
                    _short_id(cid), positions[0].get("market_question", "")[:50],
                )
                continue

            if current_rate >= min_daily:
                continue

            reason = f"reward dropped to ${current_rate:.2f}/day (min ${min_daily:.2f})"
            log.info(
                "Active reward exit %s: %s | %d open order(s) | %s",
                _short_id(cid), reason, len(positions), positions[0].get("market_question", "")[:50],
            )
            self.forget_market(cid)
            for pos in positions:
                order_id = str(pos.get("order_id") or "")
                if not order_id:
                    continue
                cancelled = await client.cancel_order(order_id)
                if cancelled:
                    await db.update_position_status(order_id, "CANCELLED")
                    self._level_share_breach_since.pop(order_id, None)

    @staticmethod
    def _daily_reward_rate(market_reward: object) -> float | None:
        configs = getattr(market_reward, "rewards_config", None) or []
        total = 0.0
        found = False
        for item in configs:
            try:
                total += float(getattr(item, "rate_per_day", 0) or 0)
                found = True
            except (TypeError, ValueError):
                continue
        return total if found else None

    # ── Enter a market ──────────────────────────────────────────────────────────

    @staticmethod
    def _book_level_usdc(order_book, side: str, price: float) -> float:
        levels = getattr(order_book, "bids" if side.upper() == "BUY" else "asks", None) or []
        total = 0.0
        for level in levels:
            try:
                level_price = float(getattr(level, "price", 0) or 0)
                level_size = float(getattr(level, "size", 0) or 0)
            except (TypeError, ValueError):
                continue
            if abs(level_price - price) <= 0.0005:
                total += level_price * level_size
        return total

    @staticmethod
    def _max_order_cost_for_level(existing_level_usdc: float, max_share_pct: float) -> float:
        share = min(99.0, max(0.0, max_share_pct)) / 100.0
        if share <= 0 or existing_level_usdc <= 0:
            return 0.0
        return existing_level_usdc * share / max(1e-9, 1.0 - share)

    @staticmethod
    def _first_position_price(
        mid_price: float,
        rewards_max_spread: float,
        bids: list[float],
    ) -> float:
        """Return the existing best bid inside the reward zone for ``first`` mode."""
        fallback = calc_order_price(mid_price, rewards_max_spread, "first")
        levels = sorted(set(round(float(price), 3) for price in bids if price), reverse=True)
        if not levels:
            return fallback

        best_bid = levels[0]
        reward_floor = max(0.001, mid_price - rewards_max_spread)
        if not reward_floor <= best_bid <= mid_price:
            return fallback
        return best_bid

    async def _replace_open_order(
        self,
        pos: dict,
        price: float,
        size: float,
        side: str | None = None,
    ) -> bool:
        """Atomically guard a cancel/place replacement from the trader loop."""
        condition_id = str(pos.get("condition_id", "") or "")
        order_id = str(pos.get("order_id", "") or "")
        if condition_id:
            self._replacing_condition_ids.add(condition_id)
        try:
            cancelled = await client.cancel_order(order_id)
            if not cancelled:
                return False

            await db.update_position_status(order_id, "CANCELLED")
            resp = await client.place_limit(pos["token_id"], price, size, side or pos["side"])
            if not resp:
                return False

            new_order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
            if not new_order_id:
                return False

            await db.upsert_position({
                **pos,
                "order_id": new_order_id,
                "price": price,
                "size": float(size),
                "side": side or pos["side"],
                "status": "OPEN",
                "placed_at": datetime.now(timezone.utc).isoformat(),
                "filled_at": None,
            })
            self._last_order_status_check.pop(order_id, None)
            return True
        finally:
            if condition_id:
                self._replacing_condition_ids.discard(condition_id)

    async def _enter_market(self, market: ScoredMarket, slot_budget: float, cfg: dict) -> float:
        """Place a single-sided BUY order on the more expensive token.
        Uses up to the fixed per-position budget.
        Returns total USDC spent."""

        if not market.tokens:
            return 0.0

        if await db.is_market_banned(market.condition_id):
            log.info("Skip '%s': market is manually banned", market.question[:40])
            self._candidates_pool.pop(market.condition_id, None)
            return 0.0

        # Pick the buy token used by the strategy.
        valid = [t for t in market.tokens if 0 < float(t["price"]) < 1]
        if not valid:
            valid = market.tokens
        token = min(valid, key=lambda t: float(t["price"]))

        token_id = token["token_id"]
        token_mid = float(token["price"])
        if not (0 < token_mid < 1):
            token_mid = market.mid_price

        depth = cfg["depth"]
        bids = market.orderbook_bids.get(token_id, [])
        target_price = calc_order_price(token_mid, market.rewards_max_spread, depth, bids=bids)

        # Guard: fetch live order book and ensure our bid is strictly below best ask.
        # post_only orders get immediately cancelled if bid >= best_ask (taker).
        live_ob = await client.get_order_book(token_id)
        if live_ob is not None:
            live_bids = _extract_bids(live_ob)
            live_asks = _extract_asks(live_ob)
            max_bid_depth = float(cfg.get("max_bid_depth_spread", 4.0))
            depth_spread = bid_depth_spread_cents(live_bids)
            if depth_spread is not None and depth_spread > max_bid_depth:
                log.info(
                    "Skip '%s': bid depth spread %.1f¢ > max %.1f¢ (on buy token)",
                    market.question[:40], depth_spread, max_bid_depth,
                )
                self._candidates_pool.pop(market.condition_id, None)
                return 0.0

            if depth == "first":
                target_price = self._first_position_price(
                    token_mid, market.rewards_max_spread, live_bids,
                )

            ask_prices = live_asks
            if ask_prices:
                best_ask = round(ask_prices[0], 2)
                if target_price >= best_ask:
                    safe_price = round(best_ask - 0.01, 2)
                    lo = max(0.01, token_mid - market.rewards_max_spread)
                    if safe_price >= lo:
                        log.info(
                            "Adjusting BUY price %.2f → %.2f (below best ask %.2f) | %s",
                            target_price, safe_price, best_ask, market.question[:50],
                        )
                        target_price = safe_price
                    else:
                        log.info(
                            "Skip '%s': bid %.2f would cross ask %.2f and no safe price in zone",
                            market.question[:40], target_price, best_ask,
                        )
                        return 0.0

        max_level_share_enabled = bool(cfg.get("target_level_share_enabled", True))
        max_level_share = float(cfg.get("max_target_level_share_pct", 50.0) or 0)
        if live_ob is not None and max_level_share_enabled and max_level_share > 0:
            existing_level_usdc = self._book_level_usdc(live_ob, "BUY", target_price)
            level_budget = self._max_order_cost_for_level(existing_level_usdc, max_level_share)
            if level_budget <= 0:
                log.info(
                    "Skip '%s': no existing liquidity at target %.2f¢ for max %.1f%% level share",
                    market.question[:40], target_price * 100, max_level_share,
                )
                return 0.0
            if level_budget < slot_budget:
                log.info(
                    "Shrink BUY budget $%.2f → $%.2f: max %.1f%% of %.2f¢ level (existing=$%.2f) | %s",
                    slot_budget, level_budget, max_level_share, target_price * 100,
                    existing_level_usdc, market.question[:45],
                )
                slot_budget = level_budget

        # Size: use as many shares as the slot budget allows, at least min_size
        min_size = math.ceil(market.rewards_min_size)
        min_cost = min_size * target_price
        if min_cost > slot_budget + 0.01:
            log.info(
                "Skip '%s': min_cost $%.2f (size=%d × %.2f¢) > budget $%.2f",
                market.question[:40], min_cost, min_size, target_price * 100, slot_budget,
            )
            return 0.0

        max_shares = math.floor(slot_budget / target_price) if target_price > 0 else min_size
        size = max(min_size, max_shares)

        order_cost = size * target_price
        if order_cost > slot_budget + 0.01:
            size = min_size
            order_cost = size * target_price
        if order_cost > slot_budget + 0.01:
            log.info(
                "Skip '%s': final cost $%.2f > budget $%.2f",
                market.question[:40], order_cost, slot_budget,
            )
            return 0.0

        log.info(
            "Placing BUY %s @ %.3f size=%.0f cost≈$%.2f (budget=$%.2f) | %s",
            token.get("outcome", "?"), target_price, size, order_cost,
            slot_budget, market.question[:60],
        )

        local_id = f"local-{uuid.uuid4().hex}"
        placed_at = datetime.now(timezone.utc).isoformat()
        await db.upsert_position({
            "order_id": local_id,
            "condition_id": market.condition_id,
            "market_question": market.question,
            "token_id": token_id,
            "outcome": token.get("outcome", ""),
            "side": "BUY",
            "price": target_price,
            "size": size,
            "status": "PENDING_PLACE",
            "placed_at": placed_at,
            "filled_at": None,
            "reward_earned": 0,
            "local_id": local_id,
            "source": "BOT",
        })

        resp = await client.place_limit(token_id, target_price, size, "BUY")
        if resp is None:
            await db.update_position_status(local_id, "FAILED")
            log.warning("Failed to place order for %s", market.condition_id)
            return 0.0
        if getattr(resp, "ok", True) is False:
            await db.update_position_status(local_id, "FAILED")
            log.warning("Rejected order for %s: %s", market.condition_id, getattr(resp, "message", "unknown"))
            return 0.0

        order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
        if not order_id:
            await db.update_position_status(local_id, "FAILED")
            log.warning("No order_id in response for %s", market.condition_id)
            return 0.0

        post_status = str(getattr(resp, "status", "") or "").upper()
        status = "FILLED" if post_status == "MATCHED" else "OPEN"
        await db.replace_position_order_id(local_id, order_id, status=status)
        return order_cost

    # ── Monitor open positions ──────────────────────────────────────────────────

    async def _monitor_positions(self, cfg: dict) -> None:
        # Recover any FILLED BUY positions whose SELL was never placed or was cancelled
        await self._recover_missing_sells(cfg)

        open_positions = await db.get_open_positions()
        if not open_positions:
            self._last_order_status_check.clear()
            return
        open_order_ids = {str(p.get("order_id") or "") for p in open_positions}
        self._last_order_status_check = {
            oid: ts for oid, ts in self._last_order_status_check.items()
            if oid in open_order_ids
        }

        depth = cfg["depth"]
        depth_changed = self._last_depth is not None and depth != self._last_depth
        if depth_changed:
            log.info("Depth changed %s → %s, re-evaluating open order prices", self._last_depth, depth)
        self._last_depth = depth

        candidate_map = {m.condition_id: m for m in self.last_scan}

        for pos in open_positions:
            order_id = pos["order_id"]
            status = ""
            status_interval = (
                ORDER_STATUS_REST_HEALTHY_WS_INTERVAL_S
                if self._user_ws.is_healthy()
                else ORDER_STATUS_REST_INTERVAL_S
            )
            if self._should_check_order_status(order_id, status_interval):
                order = await client.get_order(order_id)
                status = str(getattr(order, "status", "") or "").upper() if order else ""

            if status in ("FILLED", "MATCHED"):
                side = str(pos.get("side") or "").upper()
                log.warning("Order %s %s FILLED.", order_id, side or "?")
                await db.update_position_status(
                    order_id, "FILLED",
                    filled_at=datetime.now(timezone.utc).isoformat(),
                )
                if side == "SELL":
                    continue

                log.warning("Preparing exit sell for filled BUY %s.", order_id)
                filled_pos = {**pos, "status": "FILLED", "filled_at": datetime.now(timezone.utc).isoformat()}
                await self._place_exit_sell_for_buy(filled_pos, cfg, candidate_map, "filled_buy_sell")
                continue

            if status in ("CANCELLED", "CANCELED"):
                await db.update_position_status(order_id, "CANCELLED")
                self._level_share_breach_since.pop(order_id, None)
                continue

            # Get live order book data (used by both BUY and SELL logic)
            order_book = await self._get_order_book_ws_first(pos["token_id"], "monitor")
            live_mid = mid_from_order_book(order_book)

            # Bid/ask spread can flicker on quiet markets. Bid-depth thinning is
            # riskier: if the buy-side book no longer passes the entry filter,
            # leave the market instead of continuing to rebalance into it.
            ob_spread = _ob_spread_cents(order_book)
            max_ob_spread = float(cfg.get("max_ob_spread", 2.0))
            if ob_spread is not None and ob_spread > max_ob_spread:
                log.debug(
                    "Hold %s: spread %.1f¢ > entry max %.1f¢ (liquidity flicker) | %s",
                    order_id, ob_spread, max_ob_spread, pos.get("market_question", "")[:45],
                )

            if pos.get("side") != "SELL":
                live_bids_for_depth = _extract_bids(order_book)
                max_bid_depth = float(cfg.get("max_bid_depth_spread", 4.0))
                depth_spread = bid_depth_spread_cents(live_bids_for_depth)
                if depth_spread is not None and depth_spread > max_bid_depth:
                    log.info(
                        "Cancel %s: bid depth spread %.1f¢ > max %.1f¢ | %s",
                        order_id, depth_spread, max_bid_depth, pos.get("market_question", "")[:45],
                    )
                    cancelled = await client.cancel_order(order_id)
                    if cancelled:
                        await db.update_position_status(order_id, "CANCELLED")
                        self._candidates_pool.pop(pos["condition_id"], None)
                        self._level_share_breach_since.pop(order_id, None)
                    continue

            market_info = candidate_map.get(pos["condition_id"])
            spread = market_info.rewards_max_spread if market_info else 0.04
            current_price = pos["price"]

            # Check: market became too active or too volatile → exit BUY position
            if pos.get("side") != "SELL" and market_info is not None:
                max_daily_trades = int(cfg.get("max_daily_trades", 3))
                vol_threshold = float(cfg.get("volatility_threshold", 0.03))

                cancel_reason = None
                if market_info.trade_count > max_daily_trades:
                    cancel_reason = f"{market_info.trade_count} price changes/day > max {max_daily_trades}"
                elif market_info.price_volatility > vol_threshold:
                    cancel_reason = f"volatility {market_info.price_volatility:.4f} > threshold {vol_threshold:.3f}"

                if cancel_reason:
                    log.info(
                        "Cancel %s: %s | %s",
                        order_id, cancel_reason, pos.get("market_question", "")[:45],
                    )
                    cancelled = await client.cancel_order(order_id)
                    if cancelled:
                        await db.update_position_status(order_id, "CANCELLED")
                        self._candidates_pool.pop(pos["condition_id"], None)
                    continue

            if pos.get("side") == "SELL":
                self._level_share_breach_since.pop(order_id, None)
                # ── SELL: stay first in ask queue (lowest ask in reward zone) ──
                live_asks = _extract_asks(order_book)
                sell_mid = live_mid or current_price
                new_target = calc_sell_order_price(sell_mid, spread, live_asks)

                # Check if we're already the best ask (front of queue) — log only
                if live_asks:
                    best_ask = round(live_asks[0], 2)
                    we_are_first = abs(current_price - best_ask) < 0.005
                    if we_are_first:
                        log.debug(
                            "Sell %s already first-in-ask @ %.2f¢ | %s",
                            order_id, current_price * 100, pos.get("market_question", "")[:45],
                        )

                price_drift = abs(current_price - new_target) > 0.001
                if price_drift:
                    log.info(
                        "Sell %s reposition %.4f → %.4f (drift=%s, forced=%s) | %s",
                        order_id, current_price, new_target,
                        f"{abs(current_price - new_target):.4f}", depth_changed,
                        pos.get("market_question", "")[:45],
                    )
                    await self._replace_open_order(pos, new_target, pos["size"], "SELL")

            else:
                # ── BUY: existing bid-side logic ─────────────────────────────
                live_bids = _extract_bids(order_book)
                live_asks = _extract_asks(order_book)

                if live_mid is not None:
                    new_target = calc_order_price(live_mid, spread, depth, bids=live_bids)
                elif market_info is not None:
                    new_target = calc_order_price(market_info.mid_price, spread, depth, bids=live_bids)
                else:
                    if not depth_changed:
                        log.debug("Skip reposition %s: no OB data and not in last_scan", order_id)
                        continue
                    new_target = calc_order_price(pos["price"] / 0.97, spread, depth)

                if depth == "first" and live_mid is not None:
                    new_target = self._first_position_price(
                        live_mid, spread, live_bids,
                    )

                # ── Check 2: we're the best bid → step down to 2nd level ─────
                if depth != "first" and live_bids:
                    best_bid = round(live_bids[0], 3)
                    levels = sorted(set(round(b, 3) for b in live_bids), reverse=True)
                    we_are_first = abs(current_price - best_bid) < 0.0005
                    if we_are_first and len(levels) >= 2:
                        second_level = levels[1]
                        lo = max(0.001, (live_mid or current_price) - spread)
                        if lo <= second_level and second_level < current_price:
                            log.info(
                                "Order %s front-of-queue at %.2f¢ → step down to 2nd level %.2f¢ | %s",
                                order_id, current_price * 100, second_level * 100,
                                pos.get("market_question", "")[:45],
                            )
                            new_target = second_level

                price_drift = abs(current_price - new_target) > 0.001
                max_level_share_enabled = bool(cfg.get("target_level_share_enabled", True))
                max_level_share = float(cfg.get("max_target_level_share_pct", 50.0) or 0)
                confirm_s = max(0.0, float(cfg.get("target_level_share_confirm_s", 5) or 0))
                min_size = math.ceil(market_info.rewards_min_size) if market_info else 1

                if order_book is not None and max_level_share_enabled and max_level_share > 0 and not price_drift:
                    level_usdc = self._book_level_usdc(order_book, "BUY", current_price)
                    order_usdc = current_price * float(pos["size"])
                    target_order_usdc = max(0.0, float(cfg.get("order_usdc", 0) or 0))
                    if level_usdc + 0.01 >= order_usdc:
                        external_level_usdc = max(0.0, level_usdc - order_usdc)
                        total_after_usdc = level_usdc
                    else:
                        external_level_usdc = level_usdc
                        total_after_usdc = level_usdc + order_usdc
                    share_pct = (order_usdc / total_after_usdc * 100.0) if total_after_usdc > 0 else 100.0
                    if share_pct > max_level_share:
                        started = self._level_share_breach_since.setdefault(order_id, time.monotonic())
                        elapsed = time.monotonic() - started
                        if elapsed < confirm_s:
                            log.debug(
                                "Hold %s: level share %.1f%% > %.1f%% for %.1fs/%.0fs | %s",
                                order_id, share_pct, max_level_share, elapsed, confirm_s,
                                pos.get("market_question", "")[:45],
                            )
                            continue

                        max_cost = self._max_order_cost_for_level(external_level_usdc, max_level_share)
                        new_size = math.floor(max_cost / current_price) if current_price > 0 else 0

                        if new_size >= min_size and new_size < float(pos["size"]):
                            log.info(
                                "Shrink BUY %s size %.0f → %.0f: level share %.1f%% > %.1f%% | %s",
                                order_id, float(pos["size"]), new_size, share_pct, max_level_share,
                                pos.get("market_question", "")[:45],
                            )
                            replaced = await self._replace_open_order(pos, current_price, new_size)
                            if replaced:
                                self._level_share_breach_since.pop(order_id, None)
                            continue

                        log.info(
                            "Cancel BUY %s: level share %.1f%% > %.1f%% and allowed size %d < min %d | %s",
                            order_id, share_pct, max_level_share, new_size, min_size,
                            pos.get("market_question", "")[:45],
                        )
                        cancelled = await client.cancel_order(order_id)
                        if cancelled:
                            await db.update_position_status(order_id, "CANCELLED")
                            self._level_share_breach_since.pop(order_id, None)
                        continue

                    self._level_share_breach_since.pop(order_id, None)

                    max_cost = self._max_order_cost_for_level(external_level_usdc, max_level_share)
                    if target_order_usdc > order_usdc + 1.0 and max_cost > order_usdc + 1.0:
                        balance = await client.get_balance()
                        free_balance = float(balance)
                        buffer_pct = min(100.0, max(0.0, float(cfg.get("free_balance_buffer_pct", 10.0) or 0)))
                        free_order_cap = max(0.0, free_balance * (1.0 - buffer_pct / 100.0))
                        capital_limit = max(0.0, float(cfg.get("bot_capital_limit_usdc", 0) or 0))
                        bot_capital_in_use = await db.get_bot_capital_in_use()
                        capital_remaining = max(0.0, capital_limit - bot_capital_in_use)

                        replacement_budget = min(
                            target_order_usdc,
                            max_cost,
                            order_usdc + free_order_cap,
                            order_usdc + capital_remaining,
                        )
                        new_size = math.floor(replacement_budget / current_price) if current_price > 0 else 0
                        new_cost = new_size * current_price

                        if new_size > float(pos["size"]) and new_cost > order_usdc + 1.0:
                            log.info(
                                "Top up BUY %s size %.0f → %.0f: level can fit $%.2f under %.1f%% share | %s",
                                order_id, float(pos["size"]), new_size, new_cost, max_level_share,
                                pos.get("market_question", "")[:45],
                            )
                            await self._replace_open_order(pos, current_price, new_size)
                            continue

                if not max_level_share_enabled:
                    self._level_share_breach_since.pop(order_id, None)

                if price_drift:
                    replace_size = float(pos["size"])
                    if order_book is not None and max_level_share_enabled and max_level_share > 0:
                        existing_level_usdc = self._book_level_usdc(order_book, "BUY", new_target)
                        max_cost = self._max_order_cost_for_level(existing_level_usdc, max_level_share)
                        capped_size = math.floor(max_cost / new_target) if new_target > 0 else 0
                        if capped_size < min_size:
                            log.info(
                                "Cancel BUY %s: target %.2f¢ level cannot fit min size under %.1f%% share | %s",
                                order_id, new_target * 100, max_level_share,
                                pos.get("market_question", "")[:45],
                            )
                            cancelled = await client.cancel_order(order_id)
                            if cancelled:
                                await db.update_position_status(order_id, "CANCELLED")
                                self._level_share_breach_since.pop(order_id, None)
                            continue
                        replace_size = min(replace_size, float(capped_size))

                    log.info(
                        "Order %s reposition %.4f → %.4f size %.0f → %.0f (depth=%s, drift=%s, forced=%s)",
                        order_id, current_price, new_target, float(pos["size"]), replace_size,
                        depth, f"{abs(current_price - new_target):.4f}", depth_changed,
                    )
                    replaced = await self._replace_open_order(pos, new_target, replace_size)
                    if replaced:
                        self._level_share_breach_since.pop(order_id, None)

    # ── Recover missing sell orders ─────────────────────────────────────────────

    async def _recover_missing_sells(self, cfg: dict | None = None) -> None:
        """Find FILLED BUY positions with no open SELL and place the missing sell orders."""
        orphans = await db.get_filled_buys_without_sell()
        if not orphans:
            return
        if cfg is None:
            cfg = await self._load_cfg()

        candidate_map = {m.condition_id: m for m in self.last_scan}

        for pos in orphans:
            log.info(
                "Recovering missing SELL for filled BUY %s | %s",
                pos["order_id"], pos.get("market_question", "")[:50],
            )
            await self._place_exit_sell_for_buy(pos, cfg, candidate_map, "recover_sell")

    async def _place_exit_sell_for_buy(
        self,
        pos: dict,
        cfg: dict,
        candidate_map: dict[str, ScoredMarket],
        purpose: str,
    ) -> bool:
        ob = await self._get_order_book_ws_first(pos["token_id"], purpose)
        if ob is None:
            log.info(
                "Market resolved (no orderbook), marking BUY %s as CANCELLED | %s",
                pos["order_id"], pos.get("market_question", "")[:50],
            )
            await db.update_position_status(pos["order_id"], "CANCELLED")
            return False

        sell_mode = str(cfg.get("sell_mode") or "maker").lower()
        now = datetime.now(timezone.utc)
        if sell_mode == "market_after_delay":
            delay_s = max(0, int(cfg.get("market_sell_delay_s", 60) or 0))
            filled_at = _parse_dt(pos.get("filled_at")) or now
            elapsed_s = (now - filled_at).total_seconds()
            if elapsed_s < delay_s:
                log.debug(
                    "Wait before market SELL for BUY %s: %.0fs/%.0fs | %s",
                    pos["order_id"], elapsed_s, delay_s,
                    pos.get("market_question", "")[:50],
                )
                return True

        live_mid = mid_from_order_book(ob)
        live_asks = _extract_asks(ob)
        live_bids = _extract_bids(ob)
        market_info = candidate_map.get(pos["condition_id"])
        spread = market_info.rewards_max_spread if market_info else 0.04
        sell_mid = live_mid or pos["price"]
        sell_price = calc_sell_order_price(sell_mid, spread, live_asks)
        post_only = True
        exit_kind = "maker"

        if sell_mode == "market_after_delay" and live_bids:
            best_bid = live_bids[0]
            policy = str(cfg.get("market_sell_policy") or "always").lower()
            max_gap_cents = max(0.0, float(cfg.get("market_sell_max_gap_cents", 4.0) or 0))
            gap_cents = (float(pos["price"]) - best_bid) * 100.0
            if policy != "max_gap" or gap_cents <= max_gap_cents + 1e-9:
                sell_price = best_bid
                post_only = False
                exit_kind = "market_bid"
            else:
                log.info(
                    "Fallback to maker SELL for BUY %s: best bid %.2f¢ is %.1f¢ below buy %.2f¢ (max %.1f¢) | %s",
                    pos["order_id"], best_bid * 100, gap_cents, float(pos["price"]) * 100,
                    max_gap_cents, pos.get("market_question", "")[:50],
                )

        resp = await client.place_limit(pos["token_id"], sell_price, pos["size"], "SELL", post_only=post_only)
        if resp:
            sell_order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
            if sell_order_id:
                post_status = str(getattr(resp, "status", "") or "").upper()
                status = "FILLED" if post_status in ("MATCHED", "FILLED") else "OPEN"
                await db.upsert_position({
                    "order_id": sell_order_id,
                    "condition_id": pos["condition_id"],
                    "market_question": pos.get("market_question", ""),
                    "token_id": pos["token_id"],
                    "outcome": pos.get("outcome", ""),
                    "side": "SELL",
                    "price": sell_price,
                    "size": pos["size"],
                    "status": status,
                    "placed_at": now.isoformat(),
                    "filled_at": now.isoformat() if status == "FILLED" else None,
                    "reward_earned": 0,
                    "parent_order_id": pos["order_id"],
                    "source": pos.get("source", "BOT"),
                })
                log.info(
                    "%s SELL %s placed @ %.3f status=%s for filled BUY %s | %s",
                    exit_kind, sell_order_id, sell_price, status, pos["order_id"],
                    pos.get("market_question", "")[:50],
                )
                return True

        log.warning(
            "Cannot place SELL for %s (no tokens or balance), marking BUY as CANCELLED | %s",
            pos["order_id"], pos.get("market_question", "")[:50],
        )
        await db.update_position_status(pos["order_id"], "CANCELLED")
        return False

    # ── Config helpers ──────────────────────────────────────────────────────────

    async def _load_cfg(self) -> dict:
        from src.config import settings as s
        legacy_max_order = await db.get_setting("max_order_usdc", s.max_order_usdc)
        legacy_order_default = legacy_max_order if float(legacy_max_order or 0) > 0 else s.order_usdc
        return {
            "order_usdc":          await db.get_setting("order_usdc",          legacy_order_default),
            "bot_capital_limit_usdc": await db.get_setting("bot_capital_limit_usdc", s.bot_capital_limit_usdc),
            "free_balance_buffer_pct": await db.get_setting("free_balance_buffer_pct", s.free_balance_buffer_pct),
            "slot_pct":            await db.get_setting("slot_pct",            s.slot_pct),
            "max_slots_per_market":await db.get_setting("max_slots_per_market",s.max_slots_per_market),
            "scan_interval_s":     await db.get_setting("scan_interval_s",     s.scan_interval_s),
            "scanner_mode":        await db.get_setting("scanner_mode",        s.scanner_mode),
            "min_daily_reward":    await db.get_setting("min_daily_reward",    s.min_daily_reward),
            "depth":               await db.get_setting("depth",               s.depth),
            "category_blacklist":  await db.get_setting("category_blacklist",  s.category_blacklist),
            "volatility_threshold":await db.get_setting("volatility_threshold",s.volatility_threshold),
            "min_spread":          await db.get_setting("min_spread",          s.min_spread),
            "max_ob_spread":       await db.get_setting("max_ob_spread",       s.max_ob_spread),
            "max_daily_trades":    await db.get_setting("max_daily_trades",    s.max_daily_trades),
            "max_bid_depth_spread": await db.get_setting("max_bid_depth_spread", s.max_bid_depth_spread),
            "target_level_share_enabled": await db.get_setting("target_level_share_enabled", s.target_level_share_enabled),
            "max_target_level_share_pct": await db.get_setting("max_target_level_share_pct", s.max_target_level_share_pct),
            "target_level_share_confirm_s": await db.get_setting("target_level_share_confirm_s", s.target_level_share_confirm_s),
            "sell_mode":           await db.get_setting("sell_mode",           s.sell_mode),
            "market_sell_delay_s": await db.get_setting("market_sell_delay_s", s.market_sell_delay_s),
            "market_sell_policy":  await db.get_setting("market_sell_policy",  s.market_sell_policy),
            "market_sell_max_gap_cents": await db.get_setting("market_sell_max_gap_cents", s.market_sell_max_gap_cents),
            "monitor_interval_s":  await db.get_setting("monitor_interval_s",  s.monitor_interval_s),
            "front_run_protection": await db.get_setting("front_run_protection", s.front_run_protection),
            "front_run_bid_threshold_usd": await db.get_setting("front_run_bid_threshold_usd", s.front_run_bid_threshold_usd),
            "front_run_eat_pct":   await db.get_setting("front_run_eat_pct",   s.front_run_eat_pct),
            "front_run_window_s":  await db.get_setting("front_run_window_s",  s.front_run_window_s),
            "front_run_cooldown_s": await db.get_setting("front_run_cooldown_s", s.front_run_cooldown_s),
            "max_order_usdc":      await db.get_setting("max_order_usdc",      s.max_order_usdc),
            "max_positions":       await db.get_setting("max_positions",       s.max_positions),
            "word_blacklist":      await db.get_setting("word_blacklist",      s.word_blacklist),
        }


bot = FarmingBot()


def _short_id(value: str, head: int = 10, tail: int = 6) -> str:
    if not value:
        return "?"
    return value if len(value) <= head + tail + 1 else f"{value[:head]}...{value[-tail:]}"


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except ValueError:
        return None
