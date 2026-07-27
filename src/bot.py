"""Main farming loop."""
from __future__ import annotations

import asyncio
import logging
import math
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta

from src import db
from src.pm_client import client
from src.scanner import (
    enrich_batch, scan_markets, HybridScanner,
    calc_order_price, calc_sell_order_price,
    mid_from_order_book,
    _extract_bids, _extract_asks, _ob_spread_cents, bid_depth_spread_cents,
    multi_reward_from_api, normalize_farm_mode, normalize_both_scan_mode,
    select_buy_token_indexes, select_filter_token_indexes, ScoredMarket,
)
from src.logging_setup import configure_shadow_file_logger
from src.market_ws import MarketSignalSnapshot, MarketWsWatcher
from src.user_ws import UserWsWatcher
from src.order_manager import order_manager

log = logging.getLogger(__name__)
shadow_log = configure_shadow_file_logger()


SCAN_BATCH_SIZE  = 100   # markets enriched per tick
HYBRID_BOOK_BATCH_SIZE = 120  # cheap-passing markets checked by BUY book per tick
HYBRID_DEEP_LIMIT = 30  # markets receiving history, metadata and all-token books
REWARDS_CACHE_TTL = 1800  # seconds before refreshing full rewards list
POOL_MAX_SIZE    = 50    # best candidates kept in memory across ticks
ACTIVE_REWARD_CHECK_INTERVAL_S = 120
ACTIVE_REWARD_CHECK_DELAY_S = 0.25


MONITOR_INTERVAL = 5   # seconds between position checks
TRADE_INTERVAL = 10    # seconds between trade decisions when scanner runs separately
ORDER_STATUS_REST_INTERVAL_S = 30
ORDER_STATUS_REST_HEALTHY_WS_INTERVAL_S = 300
AUTO_RECONCILE_INTERVAL_S = 20
AMBIGUOUS_ABSENCE_GRACE_S = 60
EXIT_INVENTORY_ABSENCE_GRACE_S = 120
FAST_STEP_DEBOUNCE_S = 0.75
FAST_STEP_COOLDOWN_S = 6.0
COMPLEMENT_SHADOW_EVENT_WINDOW_S = 5.0
COMPLEMENT_SHADOW_TRADE_WINDOW_S = 3.0
COMPLEMENT_SHADOW_CANCEL_SCORE = 60


@dataclass(frozen=True)
class _ComplementShadowWatch:
    order_id: str
    condition_id: str
    token_id: str
    complement_token_id: str
    outcome: str
    order_price: float
    market_question: str


@dataclass(frozen=True)
class _BuyPlan:
    token: dict
    target_price: float
    max_budget: float
    reward_weight: float = 1.0


@dataclass(frozen=True)
class _AutoAllocation:
    sizes: tuple[int, int]
    score: float
    cost: float
    route: str


def _reward_liquidity_score(q_one: float, q_two: float, c: float = 3.0) -> float:
    """Polymarket's central-range Qmin for two complementary outcomes."""
    return max(min(q_one, q_two), q_one / c, q_two / c)


def _optimize_auto_allocation(
    plans: list[_BuyPlan],
    total_budget: float,
    min_size: int,
) -> _AutoAllocation | None:
    """Find the cheapest maximum-Qmin route: side 1, side 2, or both.

    Qmin is the maximum of three terms. Therefore its global maximum is among
    the two single-side maxima and the allocation that balances weighted
    liquidity across both outcomes. This keeps entry work constant-time.
    """
    if len(plans) != 2 or total_budget <= 0:
        return None

    prices = [max(0.0, float(plan.target_price)) for plan in plans]
    weights = [max(0.0, float(plan.reward_weight)) for plan in plans]
    max_sizes = [
        math.floor(min(total_budget, max(0.0, plan.max_budget)) / price)
        if price > 0 else 0
        for plan, price in zip(plans, prices)
    ]
    candidates: set[tuple[int, int]] = set()
    for index in range(2):
        if max_sizes[index] >= min_size and weights[index] > 0:
            sizes = [0, 0]
            sizes[index] = max_sizes[index]
            candidates.add((sizes[0], sizes[1]))

    if (
        all(size >= min_size for size in max_sizes)
        and all(weight > 0 for weight in weights)
        and min_size * sum(prices) <= total_budget + 1e-9
    ):
        # Binary-search the largest common weighted quantity q where
        # ceil(q / weight_i) shares fit both side and total-budget caps.
        low = 0.0
        high = min(
            weights[0] * max_sizes[0],
            weights[1] * max_sizes[1],
        )
        balanced = [min_size, min_size]
        for _ in range(50):
            q = (low + high) / 2
            sizes = [
                max(min_size, math.ceil(q / weights[index] - 1e-12))
                for index in range(2)
            ]
            cost = sum(size * price for size, price in zip(sizes, prices))
            if (
                cost <= total_budget + 1e-9
                and all(sizes[index] <= max_sizes[index] for index in range(2))
            ):
                low = q
                balanced = sizes
            else:
                high = q

        # Rounding down can leave cheap residual capital. Test spending it on
        # either side as well as the minimum-cost balanced point.
        candidates.add((balanced[0], balanced[1]))
        for boost_index in range(2):
            boosted = list(balanced)
            other_cost = boosted[1 - boost_index] * prices[1 - boost_index]
            boosted[boost_index] = min(
                max_sizes[boost_index],
                math.floor(max(0.0, total_budget - other_cost) / prices[boost_index]),
            )
            candidates.add((boosted[0], boosted[1]))

    best: _AutoAllocation | None = None
    for sizes in candidates:
        if any(size and size < min_size for size in sizes):
            continue
        cost = sum(size * price for size, price in zip(sizes, prices))
        if cost > total_budget + 1e-9:
            continue
        q = [sizes[index] * weights[index] for index in range(2)]
        score = _reward_liquidity_score(q[0], q[1])
        route = "both" if all(sizes) else ("cheap" if sizes[0] else "expensive")
        allocation = _AutoAllocation(sizes, score, cost, route)
        if best is None or (score, -cost) > (best.score, -best.cost):
            best = allocation
    return best


def _queue_ahead_usdc(order_book, order_price: float) -> float:
    total = 0.0
    for level in (getattr(order_book, "bids", None) or []):
        try:
            price = float(getattr(level, "price", 0) or 0)
            size = float(getattr(level, "size", 0) or 0)
        except (TypeError, ValueError):
            continue
        if price > order_price + 0.00005:
            total += price * size
    return total


def _infer_book_tick(*order_books) -> float:
    best: float | None = None
    for order_book in order_books:
        prices: list[float] = []
        for side in ("bids", "asks"):
            for level in (getattr(order_book, side, None) or []):
                try:
                    prices.append(float(getattr(level, "price", 0) or 0))
                except (TypeError, ValueError):
                    continue
        unique = sorted(set(price for price in prices if 0 < price < 1))
        for left, right in zip(unique, unique[1:]):
            diff = right - left
            if diff > 0.00005 and (best is None or diff < best):
                best = diff
    if best is None:
        return 0.01
    return min(0.01, max(0.0001, round(best, 4)))


def _complement_shadow_metrics(
    watch: _ComplementShadowWatch,
    direct_book,
    complement_book,
    direct_signal: MarketSignalSnapshot,
    complement_signal: MarketSignalSnapshot,
    previous: dict | None,
    now: float,
) -> dict:
    tick = _infer_book_tick(direct_book, complement_book)
    complement_bid = complement_signal.best_bid
    if complement_bid is None:
        return {}

    direct_mid = mid_from_order_book(direct_book)
    complement_mid = mid_from_order_book(complement_book)
    synthetic_mid = 1.0 - complement_mid if complement_mid is not None else None
    synthetic_ask = 1.0 - complement_bid
    pair_sum = watch.order_price + complement_bid
    queue_ahead = _queue_ahead_usdc(direct_book, watch.order_price)

    score = 0
    reasons: list[str] = []
    hard_risk = pair_sum >= 1.0 - tick - 1e-9
    if hard_risk:
        reasons.append("pair_sum_near_one")

    if previous and now - float(previous.get("at", 0)) <= COMPLEMENT_SHADOW_EVENT_WINDOW_S:
        previous_bid = previous.get("complement_bid")
        if previous_bid is not None and complement_bid >= float(previous_bid) + tick - 1e-9:
            score += 40
            reasons.append("complement_bid_up")

        previous_queue = float(previous.get("queue_ahead", 0) or 0)
        if previous_queue > 0:
            queue_drop_pct = max(0.0, (previous_queue - queue_ahead) / previous_queue * 100.0)
            if queue_drop_pct >= 30.0:
                score += 20
                reasons.append("queue_ahead_down")
        else:
            queue_drop_pct = 0.0
    else:
        queue_drop_pct = 0.0

    trade_age = (
        now - complement_signal.last_trade_at
        if complement_signal.last_trade_at > 0
        else None
    )
    if (
        trade_age is not None
        and trade_age <= COMPLEMENT_SHADOW_TRADE_WINDOW_S
        and complement_signal.last_trade_side.upper() == "BUY"
    ):
        score += 30
        reasons.append("complement_buy_trade")

    if (
        direct_mid is not None
        and synthetic_mid is not None
        and synthetic_mid <= direct_mid - tick + 1e-9
    ):
        score += 20
        reasons.append("synthetic_mid_lower")

    return {
        "score": score,
        "would_cancel": hard_risk or score >= COMPLEMENT_SHADOW_CANCEL_SCORE,
        "hard_risk": hard_risk,
        "reasons": reasons,
        "tick": tick,
        "direct_bid": direct_signal.best_bid,
        "direct_ask": direct_signal.best_ask,
        "direct_mid": direct_mid,
        "complement_bid": complement_bid,
        "complement_ask": complement_signal.best_ask,
        "complement_mid": complement_mid,
        "synthetic_ask": synthetic_ask,
        "synthetic_mid": synthetic_mid,
        "pair_sum": pair_sum,
        "queue_ahead_usdc": queue_ahead,
        "queue_drop_pct": queue_drop_pct,
        "last_trade_price": complement_signal.last_trade_price,
        "last_trade_side": complement_signal.last_trade_side,
        "last_trade_size": complement_signal.last_trade_size,
        "last_trade_age_s": trade_age,
        "next_previous": {
            "at": now,
            "complement_bid": complement_bid,
            "queue_ahead": queue_ahead,
        },
    }


class FarmingBot:
    def __init__(self) -> None:
        self.running = False
        self._scan_task: asyncio.Task | None = None
        self._trader_task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None
        self._fast_step_task: asyncio.Task | None = None
        self._complement_shadow_task: asyncio.Task | None = None
        self._user_ws_task: asyncio.Task | None = None
        self._market_ws_task: asyncio.Task | None = None
        self._user_ws = UserWsWatcher()
        self._market_ws = MarketWsWatcher()
        self.last_scan: list[ScoredMarket] = []
        self.shown_markets: list[ScoredMarket] = []
        self.errors: list[str] = []
        self._last_depth: str | None = None
        self._scan_lock = asyncio.Lock()
        self._trade_lock = asyncio.Lock()
        self._last_order_status_check: dict[str, float] = {}
        self._last_reconcile_at: float = 0.0
        self._level_share_breach_since: dict[str, float] = {}
        self._fast_step_cooldown_until: dict[str, float] = {}
        self._replacing_condition_ids: set[str] = set()

        # Front-run protection state
        self._fr_prev_size: dict[str, float] = {}        # token → previous best bid size (shares)
        self._fr_prev_time: dict[str, float] = {}        # token → previous timestamp
        self._fr_cooldown_until: dict[str, float] = {}   # token → cooldown expiry
        self._runtime_cfg: dict | None = None
        self._runtime_cfg_updated_at: float = 0.0
        self._active_positions_cache_initialized = False
        self._active_positions_cache_updated_at: float = 0.0
        self._active_positions_by_token: dict[str, tuple[dict, ...]] = {}
        self._active_positions_by_condition: dict[str, tuple[dict, ...]] = {}
        self._open_buy_by_token: dict[str, dict] = {}
        self._market_tokens_by_condition: dict[str, tuple[str, ...]] = {}
        self._shadow_watches_by_asset: dict[str, list[_ComplementShadowWatch]] = {}
        self._shadow_previous: dict[str, dict] = {}
        self._shadow_last_log_at: dict[str, float] = {}
        self._shadow_last_baseline_at: dict[str, float] = {}
        self._shadow_enabled = True
        self._shadow_log_interval_s = 60
        self._shadow_signal_count = 0
        self._shadow_would_cancel_count = 0
        self._last_active_reward_check_at: float = 0.0
        self._last_auto_reward_check_at: float = 0.0
        self._auto_low_share_counts: dict[str, int] = {}
        self._scan_generation: int = 0
        self._candidate_absence_by_condition: dict[str, tuple[int, int]] = {}
        order_manager.set_execution_callback(self._handle_execution_event)

        # Rotating scanner state
        self._rewards_cache: list = []
        self._rewards_cache_time: datetime | None = None
        self._rewards_cache_min_daily: float | None = None  # min_daily used when cache was built
        self._rewards_cache_scanner_mode: str | None = None
        self._candidate_pool_farm_mode: str = "cheap"
        self._candidate_pool_both_scan_mode: str = "cheap"
        self._scan_cursor: int = 0
        self._candidates_pool: dict[str, ScoredMarket] = {}  # best seen across all ticks
        self._hybrid_scanner = HybridScanner()
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
            "hybrid": {},
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
        self._complement_shadow_task = asyncio.create_task(self._complement_shadow_loop())
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
            self._complement_shadow_task,
            self._user_ws_task,
            self._market_ws_task,
        ):
            if t:
                t.cancel()
        self._scan_task = None
        self._trader_task = None
        self._monitor_task = None
        self._fast_step_task = None
        self._complement_shadow_task = None
        self._user_ws_task = None
        self._market_ws_task = None
        log.info("FarmingBot stopped")

    def forget_market(self, condition_id: str) -> None:
        """Drop a market from in-memory scanner state immediately."""
        self._candidates_pool.pop(condition_id, None)
        self.last_scan = [m for m in self.last_scan if m.condition_id != condition_id]
        self.shown_markets = [m for m in self.shown_markets if m.condition_id != condition_id]
        self._prune_market_token_cache()

    def update_runtime_settings(self, values: dict) -> None:
        """Apply an already-persisted settings update to the read-only RAM snapshot."""
        current = self._runtime_cfg or {}
        if "farm_mode" in values or "both_scan_mode" in values:
            self._handle_farm_mode_change(
                values.get("farm_mode", current.get("farm_mode", self._candidate_pool_farm_mode)),
                values.get(
                    "both_scan_mode",
                    current.get("both_scan_mode", self._candidate_pool_both_scan_mode),
                ),
            )
        if self._runtime_cfg is None:
            return
        self._runtime_cfg = {**self._runtime_cfg, **values}
        self._runtime_cfg_updated_at = time.monotonic()

    def _handle_farm_mode_change(
        self,
        farm_mode: object,
        both_scan_mode: object = "cheap",
    ) -> None:
        mode = normalize_farm_mode(farm_mode)
        scan_mode = normalize_both_scan_mode(both_scan_mode)
        if (
            mode == self._candidate_pool_farm_mode
            and scan_mode == self._candidate_pool_both_scan_mode
        ):
            return
        log.info(
            "Farm strategy changed %s/%s -> %s/%s, restarting candidate rotation",
            self._candidate_pool_farm_mode,
            self._candidate_pool_both_scan_mode,
            mode,
            scan_mode,
        )
        self._candidate_pool_farm_mode = mode
        self._candidate_pool_both_scan_mode = scan_mode
        self._scan_cursor = 0

    @staticmethod
    def _candidate_matches_farm_strategy(
        market: ScoredMarket,
        farm_mode: str,
        both_scan_mode: str,
    ) -> bool:
        if normalize_farm_mode(market.farm_mode) != farm_mode:
            return False
        return (
            farm_mode != "both"
            or normalize_both_scan_mode(market.both_scan_mode) == both_scan_mode
        )

    def invalidate_active_positions_cache(self) -> None:
        """Force one SQLite refresh after a critical order lifecycle change."""
        self._active_positions_cache_initialized = False

    async def reconcile(self) -> dict:
        """Reconcile local order journal with Polymarket open orders and holdings."""
        local_positions = await db.get_reconcilable_positions()
        open_orders, open_orders_reachable = await client.list_open_orders_status()
        if not open_orders_reachable:
            log.warning(
                "Reconcile skipped: list_open_orders is unreachable; preserving %d local states",
                len(local_positions),
            )
            return {
                "checked": 0,
                "open_orders": None,
                "open_orders_reachable": False,
                "adopted_pending": 0,
                "marked_filled": 0,
                "marked_cancelled": 0,
                "marked_unknown": 0,
            }
        open_by_id = {str(getattr(o, "id", "") or ""): o for o in open_orders}
        adopted_pending = 0
        marked_filled = 0
        marked_cancelled = 0
        marked_unknown = 0
        remote_position_sizes: dict[str, float] = {}
        positions_reachable = False
        stale_local_ambiguous = any(
            str(pos.get("status") or "").upper() == "RECONCILE_REQUIRED"
            and str(pos.get("order_id") or "").startswith("local-")
            and _age_seconds(pos.get("placed_at")) >= AMBIGUOUS_ABSENCE_GRACE_S
            for pos in local_positions
        )
        if stale_local_ambiguous:
            remote_positions, positions_reachable = await client.list_positions_status()
            if positions_reachable:
                for remote_pos in remote_positions:
                    token_id = str(getattr(remote_pos, "token_id", "") or "")
                    try:
                        remote_position_sizes[token_id] = (
                            remote_position_sizes.get(token_id, 0.0)
                            + float(getattr(remote_pos, "size", 0) or 0)
                        )
                    except (TypeError, ValueError):
                        continue

        for pos in local_positions:
            order_id = pos["order_id"]
            status = str(pos.get("status", "") or "").upper()

            if status in ("PENDING_PLACE", "RECONCILE_REQUIRED") and str(order_id).startswith("local-"):
                match = self._find_matching_open_order(pos, open_orders)
                if match is not None:
                    real_id = str(getattr(match, "id", "") or "")
                    await db.replace_position_order_id(order_id, real_id, status="OPEN")
                    adopted_pending += 1
                    continue

                # An ambiguous POST must initially be preserved because the
                # exchange may have accepted it before the connection failed.
                # Once both authoritative endpoints are reachable, the grace
                # period elapsed, no matching open order exists, and no outcome
                # tokens were acquired, the local journal row is a ghost rather
                # than an exchange order.
                token_id = str(pos.get("token_id") or "")
                if (
                    status == "RECONCILE_REQUIRED"
                    and _age_seconds(pos.get("placed_at")) >= AMBIGUOUS_ABSENCE_GRACE_S
                    and positions_reachable
                    and remote_position_sizes.get(token_id, 0.0) <= 0.0001
                ):
                    await db.update_position_status(order_id, "REMOTE_ABSENT")
                    marked_cancelled += 1
                    log.warning(
                        "Resolved ambiguous local placement as REMOTE_ABSENT "
                        "after verified absence: %s | %s",
                        order_id,
                        pos.get("market_question", "")[:60],
                    )
                    continue

                await db.update_position_status(order_id, "RECONCILE_REQUIRED")
                marked_unknown += 1
                continue

            if order_id in open_by_id:
                remote_open = open_by_id[order_id]
                matched = float(getattr(remote_open, "size_matched", 0) or 0)
                if matched > float(pos.get("matched_size") or 0) + 0.0001:
                    await order_manager.handle_order_update(
                        order_id=order_id,
                        matched_size=matched,
                        status=str(getattr(remote_open, "status", "") or ""),
                        order_type="REST_RECONCILE",
                    )
                elif status in ("PENDING_PLACE", "RECONCILE_REQUIRED", "CANCEL_PENDING", "SELL_PENDING"):
                    await db.update_position_status(order_id, "OPEN")
                continue

            order = await client.get_order(order_id)
            remote_status = str(getattr(order, "status", "") or "").upper() if order else ""
            remote_matched = float(getattr(order, "size_matched", 0) or 0) if order else 0.0
            if remote_matched > 0:
                updated = await order_manager.handle_order_update(
                    order_id=order_id,
                    matched_size=remote_matched,
                    status=remote_status,
                    order_type="REST_RECONCILE",
                )
                if updated and float(updated.get("matched_size") or 0) >= float(updated.get("size") or 0) - 0.0001:
                    marked_filled += 1
            elif remote_status in ("CANCELLED", "CANCELED"):
                await db.update_position_status(order_id, "CANCELLED")
                marked_cancelled += 1
            elif status not in ("SELL_PENDING", "EXIT_REQUIRED", "EXITING"):
                await db.update_position_status(order_id, "RECONCILE_REQUIRED")
                marked_unknown += 1

        await self._recover_missing_sells()
        self._last_reconcile_at = time.monotonic()
        return {
            "checked": len(local_positions),
            "open_orders": len(open_orders),
            "open_orders_reachable": True,
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

    async def _get_order_book_ws_first(self, token_id: str, purpose: str):
        order_book = await self._market_ws.get_order_book(token_id)
        if order_book is not None:
            return order_book
        log.info("MARKET_WS fallback_rest purpose=%s token=%s", purpose, _short_id(token_id))
        return await client.get_order_book(token_id)

    def _remember_market_tokens(self, market: ScoredMarket) -> None:
        token_ids = tuple(
            dict.fromkeys(
                str(token.get("token_id") or "")
                for token in market.tokens
                if str(token.get("token_id") or "")
            )
        )
        if len(token_ids) < 2:
            return
        self._market_tokens_by_condition[market.condition_id] = token_ids
        self._market_ws.register_market_tokens(market.condition_id, list(token_ids))

    def _prune_market_token_cache(self) -> None:
        """Keep token pairs only for current candidates and active positions."""
        retained_conditions = {
            market.condition_id for market in self.last_scan
        } | set(self._active_positions_by_condition)
        self._market_tokens_by_condition = {
            condition_id: token_ids
            for condition_id, token_ids in self._market_tokens_by_condition.items()
            if condition_id in retained_conditions
        }
        self._market_ws.replace_market_tokens(self._market_tokens_by_condition)

    def _replace_active_positions_cache(self, positions: list[dict]) -> None:
        """Atomically replace bounded active-position indexes."""
        by_token: dict[str, list[dict]] = {}
        by_condition: dict[str, list[dict]] = {}
        open_buy_by_token: dict[str, dict] = {}

        for raw_position in positions:
            position = dict(raw_position)
            token_id = str(position.get("token_id") or "")
            condition_id = str(position.get("condition_id") or "")
            if token_id:
                by_token.setdefault(token_id, []).append(position)
            if condition_id:
                by_condition.setdefault(condition_id, []).append(position)
            if (
                token_id
                and str(position.get("side") or "").upper() == "BUY"
                and str(position.get("status") or "").upper() in db.LIVE_ORDER_STATUSES
            ):
                # SQLite returns newest first; keep the first live BUY per token.
                open_buy_by_token.setdefault(token_id, position)

        self._active_positions_by_token = {
            token_id: tuple(items) for token_id, items in by_token.items()
        }
        self._active_positions_by_condition = {
            condition_id: tuple(items) for condition_id, items in by_condition.items()
        }
        self._open_buy_by_token = open_buy_by_token
        self._active_positions_cache_initialized = True
        self._active_positions_cache_updated_at = time.monotonic()
        self._prune_market_token_cache()
        open_positions = [
            position
            for positions_for_condition in self._active_positions_by_condition.values()
            for position in positions_for_condition
            if str(position.get("status") or "").upper() in db.LIVE_ORDER_STATUSES
        ]
        self._refresh_complement_shadow_watches(open_positions)

    async def _refresh_active_positions_cache(self) -> list[dict]:
        positions = await db.get_active_positions()
        self._replace_active_positions_cache(positions)
        return positions

    async def _cached_open_buy(self, token_id: str) -> dict | None:
        if not self._active_positions_cache_initialized:
            await self._refresh_active_positions_cache()
        return self._open_buy_by_token.get(str(token_id))

    async def _cached_runtime_cfg(self) -> dict:
        if self._runtime_cfg is None:
            return await self._load_cfg()
        return self._runtime_cfg

    def _refresh_complement_shadow_watches(self, positions: list[dict]) -> None:
        watches_by_asset: dict[str, list[_ComplementShadowWatch]] = {}
        active_order_ids: set[str] = set()

        for position in positions:
            if str(position.get("side") or "").upper() != "BUY":
                continue
            condition_id = str(position.get("condition_id") or "")
            token_id = str(position.get("token_id") or "")
            token_ids = self._market_tokens_by_condition.get(condition_id, ())
            complement_id = next((item for item in token_ids if item != token_id), "")
            if not token_id or not complement_id:
                continue

            watch = _ComplementShadowWatch(
                order_id=str(position.get("order_id") or ""),
                condition_id=condition_id,
                token_id=token_id,
                complement_token_id=complement_id,
                outcome=str(position.get("outcome") or ""),
                order_price=float(position.get("price") or 0),
                market_question=str(position.get("market_question") or ""),
            )
            active_order_ids.add(watch.order_id)
            watches_by_asset.setdefault(token_id, []).append(watch)
            watches_by_asset.setdefault(complement_id, []).append(watch)

        self._shadow_watches_by_asset = watches_by_asset
        self._shadow_previous = {
            order_id: state
            for order_id, state in self._shadow_previous.items()
            if order_id in active_order_ids
        }
        self._shadow_last_log_at = {
            order_id: timestamp
            for order_id, timestamp in self._shadow_last_log_at.items()
            if order_id in active_order_ids
        }
        self._shadow_last_baseline_at = {
            order_id: timestamp
            for order_id, timestamp in self._shadow_last_baseline_at.items()
            if order_id in active_order_ids
        }

    async def _complement_shadow_loop(self) -> None:
        next_periodic_at = time.monotonic() + 30.0
        while self.running:
            try:
                triggered_asset: str | None = None
                try:
                    triggered_asset = await asyncio.wait_for(
                        self._market_ws.next_shadow_changed_asset(),
                        timeout=max(0.1, next_periodic_at - time.monotonic()),
                    )
                except asyncio.TimeoutError:
                    pass

                if not self._shadow_enabled:
                    next_periodic_at = time.monotonic() + 30.0
                    continue

                if triggered_asset is not None:
                    for watch in self._shadow_watches_by_asset.get(triggered_asset, ()):
                        await self._evaluate_complement_shadow(
                            watch,
                            triggered_asset=triggered_asset,
                        )

                if time.monotonic() >= next_periodic_at:
                    unique = {
                        watch.order_id: watch
                        for watches in self._shadow_watches_by_asset.values()
                        for watch in watches
                    }
                    for watch in unique.values():
                        await self._evaluate_complement_shadow(
                            watch,
                            triggered_asset="periodic",
                            force_baseline=True,
                        )
                    next_periodic_at = time.monotonic() + 30.0
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception("Complement shadow loop error")
                await asyncio.sleep(1)

    async def _evaluate_complement_shadow(
        self,
        watch: _ComplementShadowWatch,
        *,
        triggered_asset: str,
        force_baseline: bool = False,
    ) -> None:
        direct_book = await self._market_ws.get_order_book(watch.token_id)
        complement_book = await self._market_ws.get_order_book(watch.complement_token_id)
        direct_signal = await self._market_ws.get_signal_snapshot(watch.token_id)
        complement_signal = await self._market_ws.get_signal_snapshot(
            watch.complement_token_id
        )
        if (
            direct_book is None
            or complement_book is None
            or direct_signal is None
            or complement_signal is None
        ):
            return

        now = time.monotonic()
        metrics = _complement_shadow_metrics(
            watch,
            direct_book,
            complement_book,
            direct_signal,
            complement_signal,
            self._shadow_previous.get(watch.order_id),
            now,
        )
        if not metrics:
            return
        self._shadow_previous[watch.order_id] = metrics.pop("next_previous")

        score = int(metrics["score"])
        would_cancel = bool(metrics["would_cancel"])
        baseline_due = (
            force_baseline
            and now - self._shadow_last_baseline_at.get(watch.order_id, 0.0)
            >= self._shadow_log_interval_s
        )
        has_signal = bool(metrics["reasons"])
        signal_due = has_signal and (
            would_cancel
            or now - self._shadow_last_log_at.get(watch.order_id, 0.0) >= 1.0
        )
        if not baseline_due and not signal_due:
            return

        if baseline_due:
            self._shadow_last_baseline_at[watch.order_id] = now
        if signal_due:
            self._shadow_last_log_at[watch.order_id] = now
            self._shadow_signal_count += 1
            if would_cancel:
                self._shadow_would_cancel_count += 1

        shadow_log.info(
            "COMPLEMENT_SHADOW kind=%s would_cancel=%s score=%d reasons=%s "
            "order=%s condition=%s outcome=%s order_price=%.4f "
            "direct_bid=%s direct_ask=%s direct_mid=%s "
            "complement_bid=%s complement_ask=%s complement_mid=%s "
            "synthetic_ask=%s synthetic_mid=%s pair_sum=%.4f "
            "queue_ahead_usdc=%.2f queue_drop_pct=%.1f "
            "last_trade_price=%s last_trade_side=%s last_trade_size=%.4f "
            "trigger=%s market=%s",
            "signal" if signal_due else "baseline",
            would_cancel,
            score,
            ",".join(metrics["reasons"]) or "-",
            _short_id(watch.order_id),
            _short_id(watch.condition_id),
            watch.outcome or "?",
            watch.order_price,
            _fmt_optional(metrics["direct_bid"]),
            _fmt_optional(metrics["direct_ask"]),
            _fmt_optional(metrics["direct_mid"]),
            _fmt_optional(metrics["complement_bid"]),
            _fmt_optional(metrics["complement_ask"]),
            _fmt_optional(metrics["complement_mid"]),
            _fmt_optional(metrics["synthetic_ask"]),
            _fmt_optional(metrics["synthetic_mid"]),
            metrics["pair_sum"],
            metrics["queue_ahead_usdc"],
            metrics["queue_drop_pct"],
            _fmt_optional(metrics["last_trade_price"]),
            metrics["last_trade_side"] or "?",
            metrics["last_trade_size"],
            _short_id(triggered_asset) if triggered_asset != "periodic" else "periodic",
            watch.market_question[:80],
        )

    async def ws_status(self) -> dict:
        now = time.monotonic()
        return {
            "market_ws": await self._market_ws.status(),
            "user_ws": self._user_ws.status(),
            "complement_shadow": {
                "enabled": self._shadow_enabled,
                "active_watches": len({
                    watch.order_id
                    for watches in self._shadow_watches_by_asset.values()
                    for watch in watches
                }),
                "signals_logged": self._shadow_signal_count,
                "would_cancel": self._shadow_would_cancel_count,
            },
            "runtime_cache": {
                "config_loaded": self._runtime_cfg is not None,
                "config_age_s": (
                    round(now - self._runtime_cfg_updated_at, 1)
                    if self._runtime_cfg_updated_at else None
                ),
                "active_positions": sum(
                    len(items) for items in self._active_positions_by_condition.values()
                ),
                "active_tokens": len(self._active_positions_by_token),
                "open_buy_tokens": len(self._open_buy_by_token),
                "positions_age_s": (
                    round(now - self._active_positions_cache_updated_at, 1)
                    if self._active_positions_cache_updated_at else None
                ),
                "tracked_market_pairs": len(self._market_tokens_by_condition),
            },
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
        interval = 60
        while self.running:
            try:
                cfg = await self._load_cfg()
                await self.scan_once(cfg)
                interval = int(cfg.get("scan_interval_s", 60) or 60)
            except asyncio.CancelledError:
                break
            except Exception as e:
                msg = f"{datetime.now(timezone.utc).isoformat()} {e}"
                self.errors = ([msg] + self.errors)[:50]
                log.exception("Scanner loop error: %s", e)

            await asyncio.sleep(interval)

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
                        # Leading deadline: a busy stream must not postpone risk
                        # handling forever by continuously resetting the timer.
                        pending.setdefault(asset_id, time.monotonic() + FAST_STEP_DEBOUNCE_S)
                except asyncio.TimeoutError:
                    pass

                now = time.monotonic()
                due = [asset for asset, deadline in pending.items() if deadline <= now]
                for asset_id in due:
                    pending.pop(asset_id, None)
                    cfg = await self._cached_runtime_cfg()
                    pos = await self._cached_open_buy(asset_id)
                    front_run_handled = await self._check_front_run(asset_id, cfg, pos)
                    if not front_run_handled:
                        await self._fast_step_down_asset(asset_id, cfg, pos)
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Fast step loop error: %s", e)
                await asyncio.sleep(1)

    async def _check_front_run(
        self,
        token_id: str,
        cfg: dict,
        pos: dict | None,
    ) -> bool:
        """Cancel BUY if thin level ahead is being eaten rapidly."""
        if not cfg.get("front_run_protection", True):
            return False

        now = time.monotonic()
        if now < self._fr_cooldown_until.get(token_id, 0.0):
            return False

        if pos is None:
            self._fr_prev_size.pop(token_id, None)
            self._fr_prev_time.pop(token_id, None)
            return False

        order_book = await self._market_ws.get_order_book(token_id)
        if order_book is None:
            return False

        current_price = float(pos.get("price", 0) or 0)
        live_bids = _extract_bids(order_book)
        if not live_bids:
            return False

        levels = sorted(set(round(b, 3) for b in live_bids), reverse=True)
        best_bid = levels[0]

        # If we ARE the best bid, the existing step-down logic handles it
        if abs(current_price - best_bid) < 0.0005:
            return False

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
            return False

        # Track consumption
        prev_size = self._fr_prev_size.get(token_id)
        prev_time = self._fr_prev_time.get(token_id)
        self._fr_prev_size[token_id] = best_bid_usd
        self._fr_prev_time[token_id] = now

        if prev_size is None or prev_time is None or prev_size <= 0:
            return False

        elapsed = now - prev_time
        if elapsed <= 0:
            return False

        consumed = prev_size - best_bid_usd
        if consumed <= 0:
            return False

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
            cancelled = await order_manager.cancel_order(pos["order_id"], reason="front_run")
            self.invalidate_active_positions_cache()
            if cancelled:
                self._candidates_pool.pop(pos.get("condition_id"), None)
            self._fr_cooldown_until[token_id] = time.monotonic() + cooldown_s
            self._fr_prev_size.pop(token_id, None)
            self._fr_prev_time.pop(token_id, None)
            # Do not run step-down against the same cached order after a
            # front-run cancellation attempt, including an ambiguous failure.
            return True
        return False

    async def _fast_step_down_asset(
        self,
        token_id: str,
        cfg: dict,
        pos: dict | None,
    ) -> None:
        depth = str(cfg.get("depth") or "").lower()
        if depth == "first":
            return

        now = time.monotonic()
        if now < self._fast_step_cooldown_until.get(token_id, 0.0):
            return

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
        if not live_bids:
            return

        levels = sorted(set(round(b, 3) for b in live_bids), reverse=True)
        best_bid = levels[0]
        if abs(current_price - best_bid) >= 0.0005:
            return
        if len(levels) < 2:
            log.info(
                "FAST_STEP cancel unprotected BUY %s: no bid level behind us | %s",
                pos["order_id"], pos.get("market_question", "")[:45],
            )
            await order_manager.cancel_order(
                pos["order_id"],
                reason="unprotected_best_bid",
            )
            self.invalidate_active_positions_cache()
            return

        live_mid = mid_from_order_book(order_book)
        market_info = self._candidates_pool.get(condition_id)
        if market_info is None:
            market_info = next((m for m in self.last_scan if m.condition_id == condition_id), None)
        spread = market_info.rewards_max_spread if market_info else 0.04

        second_level = levels[1]
        lo = max(0.001, (live_mid or current_price) - spread)
        if not (lo <= second_level < current_price):
            log.info(
                "FAST_STEP cancel BUY %s: next bid %.3f outside safe reward range | %s",
                pos["order_id"], second_level, pos.get("market_question", "")[:45],
            )
            await order_manager.cancel_order(
                pos["order_id"],
                reason="no_safe_step_level",
            )
            self.invalidate_active_positions_cache()
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
            self._scan_generation += 1
            for market in trade_candidates:
                self._remember_market_tokens(market)
            self._prune_market_token_cache()
            if self._active_positions_cache_initialized:
                active_positions = [
                    position
                    for items in self._active_positions_by_condition.values()
                    for position in items
                ]
                self._replace_active_positions_cache(active_positions)
        log.info(
            "Pool has %d candidates after scan; UI shows %d",
            len(trade_candidates), len(shown_candidates),
        )
        return shown_candidates

    async def trade_once(self, cfg: dict | None = None) -> None:
        async with self._trade_lock:
            await self._trade_once_locked(cfg)

    async def _trade_once_locked(self, cfg: dict | None = None) -> None:
        if cfg is None:
            cfg = await self._load_cfg()

        await self._check_active_position_rewards(cfg)
        await self._check_auto_reward_shares(cfg)

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
        farm_mode = normalize_farm_mode(cfg.get("farm_mode"))
        both_scan_mode = normalize_both_scan_mode(cfg.get("both_scan_mode"))
        entry_order_usdc = (
            min(
                order_usdc,
                max(0.01, float(cfg.get("auto_probe_usdc", 15.0) or 15.0)),
            )
            if farm_mode == "auto" else order_usdc
        )
        quote_cap_per_market = order_usdc
        capital_limit = max(0.0, float(cfg["bot_capital_limit_usdc"] or 0))
        buffer_pct = min(100.0, max(0.0, float(cfg.get("free_balance_buffer_pct", 10.0) or 0)))
        free_order_cap = max(0.0, free_balance * (1.0 - buffer_pct / 100.0))
        quote_notional = await db.get_bot_quote_notional()
        inventory_exposure = await db.get_bot_inventory_exposure()
        capital_remaining = max(0.0, capital_limit - quote_notional)
        slots_remaining = (
            math.floor(capital_remaining / quote_cap_per_market)
            if quote_cap_per_market > 0 else 0
        )
        max_slots_per_market = cfg["max_slots_per_market"]

        log.info(
            "Capital: wallet_free=$%.2f per_order_cap=$%.2f quote_notional=$%.2f quote_limit=$%.2f inventory=$%.2f | order=$%.2f remaining_slots=%d",
            free_balance, free_order_cap, quote_notional, capital_limit,
            inventory_exposure, order_usdc, slots_remaining,
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
        quote_notional = await db.get_bot_quote_notional()
        inventory_exposure = await db.get_bot_inventory_exposure()
        capital_remaining = max(0.0, capital_limit - quote_notional)
        slots_remaining = (
            math.floor(capital_remaining / quote_cap_per_market)
            if quote_cap_per_market > 0 else 0
        )

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
            if not self._candidate_matches_farm_strategy(
                market,
                farm_mode,
                both_scan_mode,
            ):
                continue

            ratio = market.score / avg_score if avg_score > 0 else 1.0
            market_budget = entry_order_usdc
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
            if quote_cap_per_market > capital_remaining + 0.01:
                log.info(
                    "Skip '%s': quote cap $%.2f > bot capital remaining $%.2f",
                    market.question[:35], quote_cap_per_market, capital_remaining,
                )
                break

            log.info(
                "Entering '%s' budget=$%.2f score=%.4f (%.1f× avg)",
                market.question[:40], market_budget, market.score, ratio,
            )
            spent = await self._enter_market(market, market_budget, cfg)
            if spent > 0:
                # Polymarket permits the same collateral to back quotes in
                # multiple markets.  Do not reserve wallet balance locally
                # after a successful quote; only the nominal quote limit is
                # consumed.  Actual fills are tracked separately as inventory.
                capital_remaining = max(0.0, capital_remaining - spent)
                slots_remaining = (
                    math.floor(capital_remaining / quote_cap_per_market)
                    if quote_cap_per_market > 0 else 0
                )
                open_condition_ids.add(market.condition_id)

    # ── Rotating scanner ───────────────────────────────────────────────────────

    async def _rotating_scan(self, cfg: dict) -> tuple[list[ScoredMarket], list[ScoredMarket]]:
        """Cycle through ALL reward markets in batches, accumulating the best in a pool."""
        now = datetime.now(timezone.utc)
        min_daily = cfg["min_daily_reward"]
        scanner_mode = str(cfg.get("scanner_mode") or "legacy").lower()
        farm_mode = normalize_farm_mode(cfg.get("farm_mode"))
        both_scan_mode = normalize_both_scan_mode(cfg.get("both_scan_mode"))
        self._handle_farm_mode_change(farm_mode, both_scan_mode)
        if scanner_mode not in ("legacy", "multi", "hybrid"):
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
            if scanner_mode in ("multi", "hybrid"):
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

        hybrid_prepare_metrics: dict = {}
        scan_source: list = self._rewards_cache
        if scanner_mode == "hybrid":
            scan_source, hybrid_prepare_metrics = self._hybrid_scanner.prepare(
                self._rewards_cache,
                min_spread=float(cfg["min_spread"]),
                order_usdc=max(0.0, float(cfg.get("order_usdc", 0) or 0)),
                farm_mode=farm_mode,
                both_scan_mode=both_scan_mode,
                word_blacklist=cfg.get("word_blacklist", []),
            )

        total = len(scan_source)
        if total == 0:
            self.scan_status.update({
                "batch_size": 0,
                "batch_scored": 0,
                "pool_size": len(self._candidates_pool),
                "shown": len(self._candidates_pool),
                "cursor": self._scan_cursor,
                "last_updated": now.isoformat(),
                "scanner_mode": scanner_mode,
                "hybrid": hybrid_prepare_metrics if scanner_mode == "hybrid" else {},
            })
            candidates = sorted(self._candidates_pool.values(), key=lambda m: m.score, reverse=True)
            return candidates, candidates
        if self._scan_cursor >= total:
            self._scan_cursor = 0

        # Take next batch from cursor (wrap around)
        batch_limit = HYBRID_BOOK_BATCH_SIZE if scanner_mode == "hybrid" else SCAN_BATCH_SIZE
        end = min(self._scan_cursor + batch_limit, total)
        batch = scan_source[self._scan_cursor:end]
        prev_cursor = self._scan_cursor
        self._scan_cursor = end % total if end < total else 0
        rotation_pct = end / total * 100
        log.info(
            "Rotating scan: cursor %d→%d / %d (%.0f%% of full rotation)",
            prev_cursor, end, total, rotation_pct,
        )

        # Enrich this batch. Hybrid has its own staged path; legacy and multi
        # stay untouched so operators retain a known rollback mode.
        hybrid_batch_metrics: dict = {}
        if scanner_mode == "hybrid":
            new_markets, hybrid_batch_metrics = await self._hybrid_scanner.scan_prepared(
                batch,
                category_blacklist=cfg["category_blacklist"],
                volatility_threshold=float(cfg["volatility_threshold"]),
                max_ob_spread=float(cfg["max_ob_spread"]),
                max_daily_trades=int(cfg["max_daily_trades"]),
                max_bid_depth_spread=float(cfg.get("max_bid_depth_spread", 4.0)),
                deep_limit=HYBRID_DEEP_LIMIT,
            )
        else:
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
                farm_mode=farm_mode,
                both_scan_mode=both_scan_mode,
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
                if not self._candidate_matches_farm_strategy(
                    m,
                    farm_mode,
                    both_scan_mode,
                )
                or m.min_order_cost <= order_usdc
            }

        # After a full rotation, retire candidates scanned for the previous
        # side, except markets with live positions that must remain managed.
        if self._scan_cursor == 0:
            active_condition_ids = {
                str(position.get("condition_id") or "")
                for position in await db.get_active_positions()
            }
            self._candidates_pool = {
                cid: market for cid, market in self._candidates_pool.items()
                if self._candidate_matches_farm_strategy(
                    market,
                    farm_mode,
                    both_scan_mode,
                )
                or cid in active_condition_ids
            }

        # Prune pool: keep top POOL_MAX_SIZE by score
        pool_sorted = sorted(self._candidates_pool.values(), key=lambda m: m.score, reverse=True)
        self._candidates_pool = {m.condition_id: m for m in pool_sorted[:POOL_MAX_SIZE]}

        capital_limit = max(0.0, float(cfg.get("bot_capital_limit_usdc", 0) or 0))
        quote_cap_per_market = order_usdc
        max_slots = (
            max(1, math.floor(capital_limit / quote_cap_per_market))
            if quote_cap_per_market > 0 else 1
        )
        shown = pool_sorted[:max_slots]
        self.scan_status.update({
            "batch_size": len(batch),
            "batch_scored": len(new_markets),
            "pool_size": len(self._candidates_pool),
            "shown": len(shown),
            "cursor": self._scan_cursor,
            "last_updated": datetime.now(timezone.utc).isoformat(),
            "scanner_mode": scanner_mode,
            "hybrid": (
                {**hybrid_prepare_metrics, **hybrid_batch_metrics}
                if scanner_mode == "hybrid"
                else {}
            ),
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

        # Treat a persisted two-sided entry as one rebalance unit. This prevents
        # a restart or mode switch from leaving half of a reward pair behind.
        open_by_group: dict[str, list[dict]] = {}
        for position in open_positions:
            if str(position.get("side") or "").upper() != "BUY":
                continue
            group_id = str(position.get("entry_group_id") or "")
            if group_id:
                open_by_group.setdefault(group_id, []).append(position)

        to_cancel: list[dict] = []
        selected_units: set[str] = set()
        for pos in scorable:
            if candidate_map[pos["condition_id"]].score >= best.score / 1.3:
                break
            group_id = str(pos.get("entry_group_id") or "")
            unit_id = group_id or str(pos.get("order_id") or "")
            if unit_id in selected_units:
                continue
            selected_units.add(unit_id)
            if group_id:
                to_cancel.extend(open_by_group.get(group_id, [pos]))
            else:
                to_cancel.append(pos)
            if len(selected_units) >= max_slots_per_market:
                break

        log.info(
            "Rebalance: cancel %d position(s) → '%s' (score=%.4f, %.2f× better than worst)",
            len(to_cancel), best.question[:40], best.score,
            best.score / max(worst_score, 1e-9),
        )
        for pos in to_cancel:
            await order_manager.cancel_order(pos["order_id"], reason="rebalance")

    # ── Exit stale positions ────────────────────────────────────────────────────

    async def _exit_stale_positions(self, candidates: list[ScoredMarket], cfg: dict | None = None) -> None:
        """Cancel open positions for markets that are expiring soon or dropped from candidates."""
        open_positions = await db.get_open_positions()
        if not open_positions:
            self._candidate_absence_by_condition.clear()
            return

        candidate_ids = {m.condition_id for m in candidates}
        open_condition_ids = {
            str(position.get("condition_id") or "")
            for position in open_positions
        }
        self._candidate_absence_by_condition = {
            cid: state
            for cid, state in self._candidate_absence_by_condition.items()
            if cid in open_condition_ids
        }
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
        drop_confirm_scans = max(
            1,
            int((cfg or {}).get("candidate_drop_confirm_scans", 3) or 3),
        )
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
            # Candidate ranking/pool rotation can flicker. Count absence only
            # once per completed scan, never once per fast trader tick.
            if not reason and cid not in candidate_ids:
                last_generation, missing_scans = (
                    self._candidate_absence_by_condition.get(cid, (-1, 0))
                )
                if last_generation != self._scan_generation:
                    missing_scans += 1
                    self._candidate_absence_by_condition[cid] = (
                        self._scan_generation,
                        missing_scans,
                    )
                    if missing_scans < drop_confirm_scans:
                        log.info(
                            "Hold %s: absent from candidates scan %d/%d | %s",
                            order_id,
                            missing_scans,
                            drop_confirm_scans,
                            pos.get("market_question", "")[:50],
                        )
                if missing_scans >= drop_confirm_scans:
                    reason = (
                        "dropped from candidates for "
                        f"{missing_scans} consecutive scans"
                    )
            elif cid in candidate_ids:
                self._candidate_absence_by_condition.pop(cid, None)
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
                if not reason.startswith("dropped from candidates"):
                    self._candidate_absence_by_condition.pop(cid, None)
                log.info("Cancelling order %s — %s | %s", order_id, reason, pos.get("market_question", "")[:50])
                await order_manager.cancel_order(order_id, reason=reason)

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
                cancelled = await order_manager.cancel_order(order_id, reason="reward_drop")
                if cancelled:
                    self._level_share_breach_since.pop(order_id, None)

    async def _check_auto_reward_shares(self, cfg: dict) -> None:
        """Adapt auto positions using one account-wide reward-share request."""
        interval_s = max(
            60,
            int(cfg.get("auto_reward_check_interval_s", 180) or 180),
        )
        now_mono = time.monotonic()
        if now_mono - self._last_auto_reward_check_at < interval_s:
            return
        # Throttle the whole path, including the SQLite lookup when no Auto
        # positions exist. Modes 1-3 therefore pay one tiny lookup per interval,
        # not one on every trader tick.
        self._last_auto_reward_check_at = now_mono

        open_positions = await db.get_open_positions()
        all_auto_by_condition: dict[str, list[dict]] = {}
        now_utc = datetime.now(timezone.utc)
        for pos in open_positions:
            if (
                str(pos.get("side") or "").upper() != "BUY"
                or normalize_farm_mode(pos.get("farm_mode")) != "auto"
            ):
                continue
            cid = str(pos.get("condition_id") or "")
            if not cid:
                continue
            all_auto_by_condition.setdefault(cid, []).append(pos)

        auto_by_condition: dict[str, list[dict]] = {}
        for cid, positions in all_auto_by_condition.items():
            placed_times = [
                placed_at
                for placed_at in (_parse_dt(pos.get("placed_at")) for pos in positions)
                if placed_at is not None
            ]
            # Judge the group only after every side/replacement has survived
            # one full reward sampling interval.
            if not placed_times or (now_utc - max(placed_times)).total_seconds() < interval_s:
                continue
            auto_by_condition[cid] = positions

        active_auto_ids = {
            str(pos.get("condition_id") or "")
            for pos in open_positions
            if normalize_farm_mode(pos.get("farm_mode")) == "auto"
        }
        self._auto_low_share_counts = {
            cid: count for cid, count in self._auto_low_share_counts.items()
            if cid in active_auto_ids
        }
        if not auto_by_condition:
            return

        percentages, reachable = await client.get_reward_percentages()
        if not reachable:
            return

        minimum = max(0.0, float(cfg.get("auto_min_reward_share_pct", 0.5) or 0))
        target = max(
            minimum,
            float(cfg.get("auto_target_reward_share_pct", 1.0) or 0),
        )
        confirmations = max(
            1,
            int(cfg.get("auto_low_share_confirmations", 2) or 2),
        )
        cooldown_s = max(
            60,
            int(cfg.get("auto_reject_cooldown_s", 21600) or 21600),
        )
        max_market_budget = max(0.0, float(cfg.get("order_usdc", 0) or 0))
        step_usdc = max(0.01, float(cfg.get("auto_step_usdc", 5.0) or 5.0))

        balance: float | None = None
        capital_remaining: float | None = None
        for cid, positions in auto_by_condition.items():
            share_pct = max(0.0, float(percentages.get(cid, 0.0) or 0))
            current_cost = sum(
                float(pos.get("price") or 0) * float(pos.get("size") or 0)
                for pos in positions
            )

            if share_pct < minimum:
                count = self._auto_low_share_counts.get(cid, 0) + 1
                self._auto_low_share_counts[cid] = count
                log.info(
                    "Auto reward share %.4f%% < %.4f%% confirmation %d/%d | %s",
                    share_pct, minimum, count, confirmations,
                    positions[0].get("market_question", "")[:50],
                )
                if count < confirmations:
                    continue

                cancelled_any = False
                for pos in positions:
                    order_id = str(pos.get("order_id") or "")
                    if order_id and await order_manager.cancel_order(
                        order_id,
                        reason="auto_low_reward_share",
                    ):
                        cancelled_any = True
                        self._level_share_breach_since.pop(order_id, None)
                if cancelled_any:
                    await db.set_condition_retry_delay(cid, cooldown_s)
                    self.forget_market(cid)
                    log.info(
                        "Auto rejected market for %.1fh after %.4f%% reward share | %s",
                        cooldown_s / 3600, share_pct,
                        positions[0].get("market_question", "")[:50],
                    )
                continue

            self._auto_low_share_counts.pop(cid, None)
            if share_pct >= target or current_cost >= max_market_budget - 0.01:
                log.debug(
                    "Auto keep %.4f%% reward share, quote=$%.2f | %s",
                    share_pct, current_cost,
                    positions[0].get("market_question", "")[:45],
                )
                continue

            desired_cost = min(max_market_budget, current_cost + step_usdc)
            if balance is None:
                balance = float(await client.get_balance())
                buffer_pct = min(
                    100.0,
                    max(0.0, float(cfg.get("free_balance_buffer_pct", 10.0) or 0)),
                )
                balance *= 1.0 - buffer_pct / 100.0
            if capital_remaining is None:
                capital_limit = max(
                    0.0,
                    float(cfg.get("bot_capital_limit_usdc", 0) or 0),
                )
                capital_remaining = max(
                    0.0,
                    capital_limit - await db.get_bot_quote_notional(),
                )

            desired_cost = min(
                desired_cost,
                balance,
                current_cost + capital_remaining,
            )
            if current_cost <= 0 or desired_cost <= current_cost + 0.99:
                continue

            ratio = desired_cost / current_cost
            max_level_share_enabled = bool(
                cfg.get("target_level_share_enabled", True)
            )
            max_level_share = float(
                cfg.get("max_target_level_share_pct", 50.0) or 0
            )
            if max_level_share_enabled and max_level_share > 0:
                # Respect the same exact-level cap as the normal monitor. For
                # a two-sided route, the tightest side limits the common scale
                # factor so its weighted allocation is not distorted.
                for pos in positions:
                    price = float(pos.get("price") or 0)
                    old_size = float(pos.get("size") or 0)
                    if price <= 0 or old_size <= 0:
                        ratio = 1.0
                        break
                    order_book = await self._get_order_book_ws_first(
                        str(pos.get("token_id") or ""),
                        "auto_top_up",
                    )
                    if order_book is None:
                        ratio = 1.0
                        break
                    level_usdc = self._book_level_usdc(
                        order_book,
                        "BUY",
                        price,
                    )
                    order_usdc = price * old_size
                    external_level_usdc = (
                        max(0.0, level_usdc - order_usdc)
                        if level_usdc + 0.01 >= order_usdc
                        else level_usdc
                    )
                    max_cost = self._max_order_cost_for_level(
                        external_level_usdc,
                        max_level_share,
                    )
                    max_size = math.floor(max_cost / price)
                    ratio = min(ratio, max_size / old_size)

            if ratio <= 1.0:
                continue
            replacements: list[tuple[dict, int]] = []
            for pos in positions:
                old_size = float(pos.get("size") or 0)
                new_size = math.floor(old_size * ratio)
                if new_size > old_size:
                    replacements.append((pos, new_size))
            if not replacements:
                continue

            added = 0.0
            for pos, new_size in replacements:
                old_cost = float(pos.get("price") or 0) * float(pos.get("size") or 0)
                new_cost = float(pos.get("price") or 0) * new_size
                if await self._replace_open_order(
                    pos,
                    float(pos.get("price") or 0),
                    new_size,
                ):
                    added += max(0.0, new_cost - old_cost)
            if added > 0:
                capital_remaining = max(0.0, capital_remaining - added)
                log.info(
                    "Auto top-up $%.2f → $%.2f after %.4f%% reward share | %s",
                    current_cost, current_cost + added, share_pct,
                    positions[0].get("market_question", "")[:50],
                )

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
        """Delegate the guarded cancel/place lifecycle to the single writer."""
        condition_id = str(pos.get("condition_id", "") or "")
        if condition_id:
            self._replacing_condition_ids.add(condition_id)
        try:
            replaced = await order_manager.replace_order(
                pos,
                price,
                size,
                side=side or pos["side"],
            )
            self._last_order_status_check.pop(str(pos.get("order_id") or ""), None)
            self.invalidate_active_positions_cache()
            return replaced
        finally:
            if condition_id:
                self._replacing_condition_ids.discard(condition_id)

    async def _prepare_buy_plan(
        self,
        market: ScoredMarket,
        token: dict,
        slot_budget: float,
        cfg: dict,
        *,
        enforce_book_quality: bool = True,
    ) -> _BuyPlan | None:
        """Validate one outcome against its live book without placing an order."""
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
        if (
            live_ob is None
            and normalize_farm_mode(cfg.get("farm_mode")) in ("both", "auto")
        ):
            log.info(
                "Skip '%s': live order book unavailable for %s side of pair",
                market.question[:40],
                token.get("outcome", "?"),
            )
            return None
        if live_ob is not None:
            live_bids = _extract_bids(live_ob)
            live_asks = _extract_asks(live_ob)
            live_spread = _ob_spread_cents(live_ob)
            max_ob_spread = float(cfg.get("max_ob_spread", 2.0))
            if (
                enforce_book_quality
                and live_spread is not None
                and live_spread > max_ob_spread
            ):
                log.info(
                    "Skip '%s': live bid-ask spread %.1f¢ > max %.1f¢ (on buy token)",
                    market.question[:40], live_spread, max_ob_spread,
                )
                self._candidates_pool.pop(market.condition_id, None)
                return None
            max_bid_depth = float(cfg.get("max_bid_depth_spread", 4.0))
            depth_spread = bid_depth_spread_cents(live_bids)
            if (
                enforce_book_quality
                and depth_spread is not None
                and depth_spread > max_bid_depth
            ):
                log.info(
                    "Skip '%s': bid depth spread %.1f¢ > max %.1f¢ (on buy token)",
                    market.question[:40], depth_spread, max_bid_depth,
                )
                self._candidates_pool.pop(market.condition_id, None)
                return None

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
                        return None

        effective_budget = slot_budget
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
                return None
            if level_budget < effective_budget:
                log.info(
                    "Shrink BUY budget $%.2f → $%.2f: max %.1f%% of %.2f¢ level (existing=$%.2f) | %s",
                    effective_budget, level_budget, max_level_share, target_price * 100,
                    existing_level_usdc, market.question[:45],
                )
                effective_budget = level_budget

        return _BuyPlan(
            token=token,
            target_price=target_price,
            max_budget=effective_budget,
            reward_weight=(
                max(
                    0.0,
                    (
                        market.rewards_max_spread
                        - abs(token_mid - target_price)
                    ) / max(market.rewards_max_spread, 1e-9),
                ) ** 2
            ),
        )

    async def _enter_market(self, market: ScoredMarket, slot_budget: float, cfg: dict) -> float:
        """Place the configured single-sided order or an equal-share YES+NO pair."""

        if not market.tokens:
            return 0.0

        if await db.is_market_banned(market.condition_id):
            log.info("Skip '%s': market is manually banned", market.question[:40])
            self._candidates_pool.pop(market.condition_id, None)
            return 0.0
        if not await order_manager.can_place(market.condition_id):
            cooldown_remaining = await order_manager.placement_cooldown_remaining(
                market.condition_id
            )
            log.debug(
                "Skip '%s': placement cooldown %.0fs",
                market.question[:40],
                cooldown_remaining,
            )
            return 0.0

        farm_mode = normalize_farm_mode(cfg.get("farm_mode"))
        both_scan_mode = normalize_both_scan_mode(cfg.get("both_scan_mode"))
        valid = [token for token in market.tokens if 0 < float(token["price"]) < 1]
        if not valid:
            valid = market.tokens
        if farm_mode == "auto":
            selected_indexes = tuple(sorted(
                range(len(valid)),
                key=lambda index: float(valid[index].get("price", 0) or 0),
            ))
        else:
            selected_indexes = select_buy_token_indexes(valid, farm_mode)
        if farm_mode == "both" and len(selected_indexes) != 2:
            log.info(
                "Skip '%s': two-sided mode requires exactly two valid outcomes",
                market.question[:40],
            )
            return 0.0
        if farm_mode == "auto" and len(selected_indexes) != 2:
            log.info(
                "Skip '%s': auto mode requires exactly two valid outcomes",
                market.question[:40],
            )
            return 0.0

        plans: list[_BuyPlan] = []
        quality_indexes = set(select_filter_token_indexes(
            valid,
            farm_mode,
            both_scan_mode,
        ))
        for index in selected_indexes:
            plan = await self._prepare_buy_plan(
                market,
                valid[index],
                slot_budget,
                cfg,
                enforce_book_quality=index in quality_indexes,
            )
            if plan is None:
                return 0.0
            plans.append(plan)

        min_size = math.ceil(market.rewards_min_size)
        if farm_mode == "auto":
            allocation = _optimize_auto_allocation(plans, slot_budget, min_size)
            if allocation is None:
                log.info(
                    "Skip '%s': no qualifying auto allocation under $%.2f",
                    market.question[:40], slot_budget,
                )
                return 0.0
            plan_sizes = list(zip(plans, allocation.sizes))
            plan_sizes = [(plan, size) for plan, size in plan_sizes if size > 0]
            log.info(
                "Auto route=%s Qmin=%.3f cost=$%.2f/%0.2f | %s",
                allocation.route, allocation.score, allocation.cost, slot_budget,
                market.question[:50],
            )
        else:
            # A pair uses the same share count on both outcomes. The more
            # expensive or thinner side is therefore the limiting side.
            max_sizes = [
                math.floor(plan.max_budget / plan.target_price)
                if plan.target_price > 0 else 0
                for plan in plans
            ]
            combined_price = sum(plan.target_price for plan in plans)
            total_budget_size = (
                math.floor(slot_budget / combined_price)
                if combined_price > 0 else 0
            )
            max_sizes.append(total_budget_size)
            size = min(max_sizes, default=0)
            if size < min_size:
                log.info(
                    "Skip '%s': equal size %d < reward minimum %d "
                    "(combined price=%.2f¢, market budget=$%.2f)",
                    market.question[:40], size, min_size,
                    combined_price * 100,
                    slot_budget,
                )
                return 0.0
            plan_sizes = [(plan, size) for plan in plans]

        entry_group_id = (
            f"{'auto' if farm_mode == 'auto' else 'pair'}-{uuid.uuid4().hex}"
            if farm_mode in ("both", "auto") else None
        )
        # Place the higher-priced side first: it is normally the tighter budget
        # constraint. If the second placement fails, roll back confirmed orders.
        plan_sizes.sort(key=lambda item: item[0].target_price, reverse=True)
        successful: list[dict] = []
        total_cost = 0.0
        for plan, size in plan_sizes:
            token = plan.token
            order_cost = size * plan.target_price
            log.info(
                "Placing BUY %s @ %.3f size=%.0f cost≈$%.2f (side budget=$%.2f) | %s",
                token.get("outcome", "?"), plan.target_price, size, order_cost,
                plan.max_budget, market.question[:60],
            )

            local_id = f"local-{uuid.uuid4().hex}"
            resp, placed = await order_manager.place_position({
                "order_id": local_id,
                "condition_id": market.condition_id,
                "market_question": market.question,
                "token_id": token["token_id"],
                "outcome": token.get("outcome", ""),
                "side": "BUY",
                "price": plan.target_price,
                "size": size,
                "status": "PENDING_PLACE",
                "placed_at": datetime.now(timezone.utc).isoformat(),
                "filled_at": None,
                "matched_size": 0,
                "reward_earned": 0,
                "local_id": local_id,
                "source": "BOT",
                "farm_mode": farm_mode,
                "entry_group_id": entry_group_id,
            })
            self.invalidate_active_positions_cache()
            order_id = str(placed.get("order_id") or "")
            failed = (
                resp is None
                or bool(getattr(resp, "ambiguous", False))
                or getattr(resp, "ok", True) is False
                or not order_id
            )
            if failed:
                if resp is None or bool(getattr(resp, "ambiguous", False)):
                    log.warning(
                        "Ambiguous placement for %s; left in %s for reconciliation",
                        market.condition_id,
                        placed.get("status"),
                    )
                else:
                    log.warning(
                        "Rejected order for %s code=%s: %s",
                        market.condition_id,
                        getattr(resp, "error_code", "") or getattr(resp, "code", "unknown"),
                        getattr(resp, "error_message", "") or getattr(resp, "message", "unknown"),
                    )
                for prior in successful:
                    await order_manager.cancel_order(
                        prior["order_id"],
                        reason="paired_entry_rollback",
                    )
                return 0.0

            successful.append(placed)
            total_cost += order_cost

        return total_cost

    # ── Monitor open positions ──────────────────────────────────────────────────

    async def _monitor_positions(self, cfg: dict) -> None:
        # Recover any FILLED BUY positions whose SELL was never placed or was cancelled
        await self._recover_missing_sells(cfg)

        self._shadow_enabled = bool(cfg.get("complement_shadow_enabled", True))
        self._shadow_log_interval_s = max(
            10,
            int(cfg.get("complement_shadow_log_interval_s", 60) or 60),
        )
        active_positions = await self._refresh_active_positions_cache()
        open_positions = [
            position
            for position in active_positions
            if str(position.get("status") or "").upper() in db.LIVE_ORDER_STATUSES
        ]
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
            order = None
            status_interval = (
                ORDER_STATUS_REST_HEALTHY_WS_INTERVAL_S
                if self._user_ws.is_healthy()
                else ORDER_STATUS_REST_INTERVAL_S
            )
            if self._should_check_order_status(order_id, status_interval):
                order = await client.get_order(order_id)
                status = str(getattr(order, "status", "") or "").upper() if order else ""

            if order is not None:
                remote_matched = float(getattr(order, "size_matched", 0) or 0)
                if remote_matched > float(pos.get("matched_size") or 0) + 0.0001:
                    await order_manager.handle_order_update(
                        order_id=order_id,
                        matched_size=remote_matched,
                        status=status,
                        order_type="REST_MONITOR",
                    )
                    continue

            if status in ("CANCELLED", "CANCELED"):
                await order_manager.handle_order_update(
                    order_id=order_id,
                    matched_size=float(pos.get("matched_size") or 0),
                    status=status,
                    order_type="CANCELLATION",
                )
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
                    cancelled = await order_manager.cancel_order(order_id, reason="thin_bid_depth")
                    if cancelled:
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
                    cancelled = await order_manager.cancel_order(order_id, reason=cancel_reason)
                    if cancelled:
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
                        cancelled = await order_manager.cancel_order(order_id, reason="level_share")
                        if cancelled:
                            self._level_share_breach_since.pop(order_id, None)
                        continue

                    self._level_share_breach_since.pop(order_id, None)

                    max_cost = self._max_order_cost_for_level(external_level_usdc, max_level_share)
                    grouped_entry = (
                        normalize_farm_mode(pos.get("farm_mode")) in ("both", "auto")
                        and bool(pos.get("entry_group_id"))
                    )
                    if (
                        not grouped_entry
                        and target_order_usdc > order_usdc + 1.0
                        and max_cost > order_usdc + 1.0
                    ):
                        balance = await client.get_balance()
                        free_balance = float(balance)
                        buffer_pct = min(100.0, max(0.0, float(cfg.get("free_balance_buffer_pct", 10.0) or 0)))
                        free_order_cap = max(0.0, free_balance * (1.0 - buffer_pct / 100.0))
                        capital_limit = max(0.0, float(cfg.get("bot_capital_limit_usdc", 0) or 0))
                        quote_notional = await db.get_bot_quote_notional()
                        capital_remaining = max(0.0, capital_limit - quote_notional)

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
                            cancelled = await order_manager.cancel_order(order_id, reason="target_level_too_small")
                            if cancelled:
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

    async def _handle_execution_event(self, pos: dict, reason: str) -> None:
        """Fast execution callback from the single OrderManager.

        Partial BUYs bypass the configured maker delay: first cancel the BUY
        remainder (done by OrderManager), then liquidate exactly the acquired
        inventory.  A partial SELL re-enters here through its parent BUY and
        continues only with the unsold remainder.
        """
        self.invalidate_active_positions_cache()
        side = str(pos.get("side") or "").upper()
        buy = pos
        if side == "SELL":
            parent_id = str(pos.get("parent_order_id") or "")
            buy = await db.get_position(parent_id) if parent_id else None
            if buy is None:
                return

        cfg = await self._load_cfg()
        candidate_map = {m.condition_id: m for m in self.last_scan}
        immediate = reason.startswith("partial")
        purpose = "partial_fill_rescue" if immediate else "filled_buy_sell"

        # Settlement/position endpoints may trail the WS match by a few seconds.
        # Retry without blocking the WS receiver (OrderManager schedules us).
        for delay in (0.0, 0.5, 1.0, 2.0, 4.0):
            if delay:
                await asyncio.sleep(delay)
            placed = await self._place_exit_sell_for_buy(
                buy,
                cfg,
                candidate_map,
                purpose,
                force_immediate=immediate,
            )
            if placed:
                return
        log.warning(
            "Exit still required after execution callback BUY=%s reason=%s",
            buy.get("order_id"),
            reason,
        )

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
        *,
        force_immediate: bool = False,
    ) -> bool:
        accounting = await db.get_parent_exit_accounting(pos["order_id"])
        uncovered = float(accounting["uncovered"])
        if uncovered <= 0.0001:
            if float(accounting["acquired"]) > 0 and float(accounting["working_sell"]) <= 0.0001:
                await db.update_position_status(
                    pos["order_id"],
                    "EXITED",
                )
            return True

        actual_tokens, positions_reachable = await client.get_token_position_size_status(pos["token_id"])
        if not positions_reachable:
            log.warning("Cannot verify token balance for exit BUY %s; will retry", pos["order_id"])
            return False
        all_working_sells = await db.get_working_sell_remaining(pos["token_id"])
        execution_at = (
            _parse_dt(pos.get("filled_at"))
            or _parse_dt(pos.get("placed_at"))
        )
        execution_age_s = (
            (datetime.now(timezone.utc) - execution_at).total_seconds()
            if execution_at is not None
            else 0.0
        )
        if (
            actual_tokens <= 0.0001
            and all_working_sells <= 0.0001
            and execution_age_s >= EXIT_INVENTORY_ABSENCE_GRACE_S
        ):
            # A manually sold or otherwise externally closed inventory has no
            # child SELL in our journal. The reachable Position API is the
            # authority here; retire the stale parent instead of retrying it
            # forever as EXIT_REQUIRED.
            await db.update_position_status(pos["order_id"], "EXITED")
            self.invalidate_active_positions_cache()
            log.info(
                "Mark EXITED: no remote inventory and no working SELL for BUY %s "
                "(execution age %.0fs) | %s",
                pos["order_id"],
                execution_age_s,
                pos.get("market_question", "")[:50],
            )
            return True
        available_tokens = max(0.0, actual_tokens - all_working_sells)
        exit_size = min(uncovered, available_tokens)
        if exit_size <= 0.0001:
            log.debug(
                "Exit BUY %s waiting for settled inventory: need=%.6f actual=%.6f working_sell=%.6f",
                pos["order_id"], uncovered, actual_tokens, all_working_sells,
            )
            return False

        ob = await self._get_order_book_ws_first(pos["token_id"], purpose)
        if ob is None:
            log.warning(
                "No orderbook for exit BUY %s; keeping EXIT_REQUIRED | %s",
                pos["order_id"], pos.get("market_question", "")[:50],
            )
            await db.update_position_status(pos["order_id"], "EXIT_REQUIRED")
            return False

        sell_mode = str(cfg.get("sell_mode") or "maker").lower()
        now = datetime.now(timezone.utc)
        if sell_mode == "market_after_delay" and not force_immediate:
            delay_s = max(0, int(cfg.get("market_sell_delay_s", 60) or 0))
            filled_at = (
                _parse_dt(pos.get("filled_at"))
                or _parse_dt(pos.get("placed_at"))
            )
            # A legacy row may have lost filled_at. Never restart the delay on
            # every recovery cycle; placed_at makes the exit immediately due.
            elapsed_s = (
                (now - filled_at).total_seconds()
                if filled_at is not None
                else float(delay_s)
            )
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
        min_order_size = _positive_float(getattr(ob, "min_order_size", None))
        if min_order_size is None:
            rest_book = await client.get_order_book(pos["token_id"])
            min_order_size = _positive_float(
                getattr(rest_book, "min_order_size", None)
            )
        market_info = candidate_map.get(pos["condition_id"])
        spread = market_info.rewards_max_spread if market_info else 0.04
        sell_mid = live_mid or pos["price"]
        sell_price = calc_sell_order_price(sell_mid, spread, live_asks)
        post_only = True
        exit_kind = "maker"
        market_sell = False
        market_min_price: float | None = None

        if (force_immediate or sell_mode == "market_after_delay") and live_bids:
            best_bid = live_bids[0]
            policy = str(cfg.get("market_sell_policy") or "always").lower()
            max_gap_cents = max(0.0, float(cfg.get("market_sell_max_gap_cents", 4.0) or 0))
            gap_cents = (float(pos["price"]) - best_bid) * 100.0
            if force_immediate or policy != "max_gap" or gap_cents <= max_gap_cents + 1e-9:
                sell_price = best_bid
                post_only = False
                exit_kind = "partial_rescue" if force_immediate else "market_bid"
            else:
                log.info(
                    "Fallback to maker SELL for BUY %s: best bid %.2f¢ is %.1f¢ below buy %.2f¢ (max %.1f¢) | %s",
                    pos["order_id"], best_bid * 100, gap_cents, float(pos["price"]) * 100,
                    max_gap_cents, pos.get("market_question", "")[:50],
                )

        if (
            min_order_size is not None
            and exit_size + 1e-9 < min_order_size
        ):
            if not live_bids:
                log.warning(
                    "Dust exit %.6f is below min order %.6f but book has no bid; "
                    "keeping EXIT_REQUIRED | %s",
                    exit_size,
                    min_order_size,
                    pos.get("market_question", "")[:50],
                )
                await db.update_position_status(pos["order_id"], "EXIT_REQUIRED")
                return False
            sell_price = live_bids[0]
            post_only = False
            market_sell = True
            market_min_price = sell_price
            exit_kind = "dust_fak"

        local_id = f"local-sell-{uuid.uuid4().hex}"
        resp, sell_pos = await order_manager.place_position(
            {
                "order_id": local_id,
                "condition_id": pos["condition_id"],
                "market_question": pos.get("market_question", ""),
                "token_id": pos["token_id"],
                "outcome": pos.get("outcome", ""),
                "side": "SELL",
                "price": sell_price,
                "size": exit_size,
                "status": "SELL_PENDING",
                "placed_at": now.isoformat(),
                "filled_at": None,
                "matched_size": 0,
                "reward_earned": 0,
                "parent_order_id": pos["order_id"],
                "source": pos.get("source", "BOT"),
            },
            post_only=post_only,
            pending_status="SELL_PENDING",
            market_sell=market_sell,
            min_price=market_min_price,
        )
        if resp and getattr(resp, "ok", True) is not False:
            sell_order_id = str(sell_pos.get("order_id") or "")
            if sell_order_id:
                await db.update_position_status(pos["order_id"], "EXITING")
                log.info(
                    "%s SELL %s placed @ %.3f size=%.6f state=%s for BUY %s | %s",
                    exit_kind, sell_order_id, sell_price, exit_size,
                    sell_pos.get("status"), pos["order_id"],
                    pos.get("market_question", "")[:50],
                )
                return True

        log.warning(
            "Cannot place SELL for %s; keeping EXIT_REQUIRED | %s",
            pos["order_id"], pos.get("market_question", "")[:50],
        )
        await db.update_position_status(pos["order_id"], "EXIT_REQUIRED")
        return False

    # ── Config helpers ──────────────────────────────────────────────────────────

    async def _load_cfg(self) -> dict:
        from src.config import settings as s
        stored = await db.get_all_settings()
        legacy_max_order = stored.get("max_order_usdc", s.max_order_usdc)
        legacy_order_default = legacy_max_order if float(legacy_max_order or 0) > 0 else s.order_usdc
        defaults = {
            "order_usdc":          legacy_order_default,
            "bot_capital_limit_usdc": s.bot_capital_limit_usdc,
            "free_balance_buffer_pct": s.free_balance_buffer_pct,
            "slot_pct":            s.slot_pct,
            "max_slots_per_market":s.max_slots_per_market,
            "scan_interval_s":     s.scan_interval_s,
            "scanner_mode":        s.scanner_mode,
            "farm_mode":           s.farm_mode,
            "both_scan_mode":      s.both_scan_mode,
            "auto_probe_usdc":     s.auto_probe_usdc,
            "auto_min_reward_share_pct": s.auto_min_reward_share_pct,
            "auto_target_reward_share_pct": s.auto_target_reward_share_pct,
            "auto_step_usdc":      s.auto_step_usdc,
            "auto_reward_check_interval_s": s.auto_reward_check_interval_s,
            "auto_low_share_confirmations": s.auto_low_share_confirmations,
            "auto_reject_cooldown_s": s.auto_reject_cooldown_s,
            "min_daily_reward":    s.min_daily_reward,
            "depth":               s.depth,
            "category_blacklist":  s.category_blacklist,
            "volatility_threshold":s.volatility_threshold,
            "min_spread":          s.min_spread,
            "max_ob_spread":       s.max_ob_spread,
            "max_daily_trades":    s.max_daily_trades,
            "max_bid_depth_spread": s.max_bid_depth_spread,
            "target_level_share_enabled": s.target_level_share_enabled,
            "max_target_level_share_pct": s.max_target_level_share_pct,
            "target_level_share_confirm_s": s.target_level_share_confirm_s,
            "sell_mode":           s.sell_mode,
            "market_sell_delay_s": s.market_sell_delay_s,
            "market_sell_policy":  s.market_sell_policy,
            "market_sell_max_gap_cents": s.market_sell_max_gap_cents,
            "monitor_interval_s":  s.monitor_interval_s,
            "candidate_drop_confirm_scans": s.candidate_drop_confirm_scans,
            "front_run_protection": s.front_run_protection,
            "front_run_bid_threshold_usd": s.front_run_bid_threshold_usd,
            "front_run_eat_pct":   s.front_run_eat_pct,
            "front_run_window_s":  s.front_run_window_s,
            "front_run_cooldown_s": s.front_run_cooldown_s,
            "complement_shadow_enabled": s.complement_shadow_enabled,
            "complement_shadow_log_interval_s": s.complement_shadow_log_interval_s,
            "max_order_usdc":      s.max_order_usdc,
            "max_positions":       s.max_positions,
            "word_blacklist":      s.word_blacklist,
        }
        cfg = {key: stored.get(key, default) for key, default in defaults.items()}
        self._runtime_cfg = cfg
        self._runtime_cfg_updated_at = time.monotonic()
        return cfg


bot = FarmingBot()


def _short_id(value: str, head: int = 10, tail: int = 6) -> str:
    if not value:
        return "?"
    return value if len(value) <= head + tail + 1 else f"{value[:head]}...{value[-tail:]}"


def _fmt_optional(value: float | None) -> str:
    return "-" if value is None else f"{value:.4f}"


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


def _age_seconds(value: str | None) -> float:
    placed_at = _parse_dt(value)
    if placed_at is None:
        return 0.0
    return max(0.0, (datetime.now(timezone.utc) - placed_at).total_seconds())


def _positive_float(value: object) -> float | None:
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None
