"""Main farming loop."""
from __future__ import annotations

import asyncio
import logging
import math
from datetime import datetime, timezone, timedelta
from decimal import Decimal

from src import db
from src.pm_client import client
from src.scanner import (
    enrich_batch, scan_markets,
    calc_order_price, calc_sell_order_price,
    mid_from_order_book,
    _extract_bids, _extract_asks, _ob_spread_cents,
    ScoredMarket,
)

log = logging.getLogger(__name__)


SCAN_BATCH_SIZE  = 100   # markets enriched per tick
REWARDS_CACHE_TTL = 1800  # seconds before refreshing full rewards list
POOL_MAX_SIZE    = 50    # best candidates kept in memory across ticks


MONITOR_INTERVAL = 5   # seconds between position checks


class FarmingBot:
    def __init__(self) -> None:
        self.running = False
        self._task: asyncio.Task | None = None
        self._monitor_task: asyncio.Task | None = None
        self.last_scan: list[ScoredMarket] = []
        self.errors: list[str] = []
        self._last_depth: str | None = None

        # Rotating scanner state
        self._rewards_cache: list = []
        self._rewards_cache_time: datetime | None = None
        self._rewards_cache_min_daily: float | None = None  # min_daily used when cache was built
        self._scan_cursor: int = 0
        self._candidates_pool: dict[str, ScoredMarket] = {}  # best seen across all ticks

    # ── Public API ──────────────────────────────────────────────────────────────

    def start(self) -> None:
        if self.running:
            return
        self.running = True
        self._task = asyncio.create_task(self._loop())
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        log.info("FarmingBot started")

    def stop(self) -> None:
        self.running = False
        for t in (self._task, self._monitor_task):
            if t:
                t.cancel()
        self._task = None
        self._monitor_task = None
        log.info("FarmingBot stopped")

    # ── Main loop ───────────────────────────────────────────────────────────────

    async def _loop(self) -> None:
        while self.running:
            try:
                cfg = await self._load_cfg()
                await self.tick(cfg)
            except asyncio.CancelledError:
                break
            except Exception as e:
                msg = f"{datetime.now(timezone.utc).isoformat()} {e}"
                self.errors = ([msg] + self.errors)[:50]
                log.exception("Bot tick error: %s", e)

            interval = await db.get_setting("scan_interval_s", 60)
            await asyncio.sleep(int(interval))

    async def _monitor_loop(self) -> None:
        """Fast loop: check open positions every monitor_interval_s seconds.
        Handles fills, cancellations, and price drift independently of the main scan."""
        await asyncio.sleep(MONITOR_INTERVAL)  # let main loop init first
        while self.running:
            try:
                cfg = await self._load_cfg()
                await self._monitor_positions(cfg)
                interval = int(cfg.get("monitor_interval_s", MONITOR_INTERVAL))
            except asyncio.CancelledError:
                break
            except Exception as e:
                log.exception("Monitor loop error: %s", e)
                interval = MONITOR_INTERVAL
            await asyncio.sleep(interval)

    async def tick(self, cfg: dict | None = None) -> None:
        if cfg is None:
            cfg = await self._load_cfg()

        # 1. Rotating scan — process next batch, merge into persistent pool
        candidates = await self._rotating_scan(cfg)
        self.last_scan = candidates
        log.info("Pool has %d candidates after this tick", len(candidates))

        # 2. Exit stale positions (expiring soon, dropped from candidates, or reward dropped)
        await self._exit_stale_positions(candidates, cfg)

        balance = await client.get_balance()
        await db.record_balance(float(balance))

        # Capital accounting: total = free USDC + locked in open BUY orders
        locked_usdc = await client.get_locked_usdc()
        total_capital = float(balance) + locked_usdc

        slot_pct = cfg["slot_pct"]
        _max_positions = int(cfg.get("max_positions", 0) or 0)
        max_slots = _max_positions if _max_positions > 0 else max(1, int(100 / slot_pct))
        max_slots_per_market = cfg["max_slots_per_market"]
        slot_value = total_capital * slot_pct / 100.0  # USDC per slot
        _max_order_usdc = float(cfg.get("max_order_usdc", 0) or 0)
        if _max_order_usdc > 0:
            slot_value = _max_order_usdc  # fixed order size drives all slot accounting

        # Slots already consumed by open positions
        slots_used = round(locked_usdc / slot_value) if slot_value > 0 else 0
        slots_remaining = max(0, max_slots - slots_used)

        log.info(
            "Capital: free=$%.2f locked=$%.2f total=$%.2f | slot=$%.2f used=%d/%d remaining=%d",
            float(balance), locked_usdc, total_capital,
            slot_value, slots_used, max_slots, slots_remaining,
        )

        # 3. Rebalance: cancel worst positions to make room for significantly better ones
        await self._rebalance_risk(candidates, slots_remaining, slot_value, max_slots_per_market)

        # 4. Re-fetch and enter new markets
        open_positions = await db.get_open_positions()
        open_condition_ids = {p["condition_id"] for p in open_positions}

        # Recompute after potential rebalance
        locked_usdc = await client.get_locked_usdc()
        slots_used = round(locked_usdc / slot_value) if slot_value > 0 else 0
        slots_remaining = max(0, max_slots - slots_used)

        # Average score for super-deal detection
        avg_score = (sum(m.score for m in candidates) / len(candidates)) if candidates else 0

        log.info(
            "Enter loop: %d candidates, %d slots remaining, avg_score=%.4f",
            len(candidates), slots_remaining, avg_score,
        )

        for market in candidates:
            if slots_remaining <= 0:
                break
            if market.condition_id in open_condition_ids:
                continue

            # Determine slot multiplier based on score vs pool average
            ratio = market.score / avg_score if avg_score > 0 else 1.0
            if ratio >= 4.0 and slots_remaining >= 4:
                num_slots = min(4, max_slots_per_market, slots_remaining)
            elif ratio >= 3.0 and slots_remaining >= 3:
                num_slots = min(3, max_slots_per_market, slots_remaining)
            elif ratio >= 2.0 and slots_remaining >= 2:
                num_slots = min(2, max_slots_per_market, slots_remaining)
            else:
                num_slots = 1

            market_budget = slot_value * num_slots
            _max_order_usdc = float(cfg.get("max_order_usdc", 0) or 0)
            if _max_order_usdc > 0:
                market_budget = _max_order_usdc  # fixed order size set by user
            if market_budget < market.min_order_cost:
                log.info(
                    "Skip '%s': budget $%.2f < min_cost $%.2f",
                    market.question[:35], market_budget, market.min_order_cost,
                )
                continue

            log.info(
                "Entering '%s' slots=%d budget=$%.2f score=%.4f (%.1f× avg)",
                market.question[:40], num_slots, market_budget, market.score, ratio,
            )
            spent = await self._enter_market(market, market_budget, cfg["depth"])
            if spent > 0:
                slots_remaining -= num_slots
                open_condition_ids.add(market.condition_id)

    # ── Rotating scanner ───────────────────────────────────────────────────────

    async def _rotating_scan(self, cfg: dict) -> list[ScoredMarket]:
        """Cycle through ALL reward markets in batches, accumulating the best in a pool."""
        now = datetime.now(timezone.utc)
        min_daily = cfg["min_daily_reward"]

        # Refresh the full rewards list every REWARDS_CACHE_TTL seconds
        cache_age = (now - self._rewards_cache_time).total_seconds() if self._rewards_cache_time else 9999
        min_daily_changed = self._rewards_cache_min_daily != min_daily
        if not self._rewards_cache or cache_age > REWARDS_CACHE_TTL or min_daily_changed:
            if min_daily_changed and self._rewards_cache:
                log.info("min_daily_reward changed %.1f → %.1f, forcing cache refresh + pool reset", self._rewards_cache_min_daily, min_daily)
                self._candidates_pool.clear()
            from src.pm_client import client as _c
            all_rewards = await _c.get_all_rewards()
            self._rewards_cache = [
                r for r in all_rewards
                if float(getattr(r, "total_daily_rate", 0) or 0) >= min_daily
            ]
            self._rewards_cache_time = now
            self._rewards_cache_min_daily = min_daily
            self._scan_cursor = 0
            log.info("Rewards cache refreshed: %d markets pass min_daily>=%.1f", len(self._rewards_cache), min_daily)

        total = len(self._rewards_cache)
        if total == 0:
            return list(self._candidates_pool.values())

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
        )

        # Merge into pool: update existing + add new
        for m in new_markets:
            self._candidates_pool[m.condition_id] = m

        # Apply user word blacklist — purge matching markets from pool in real-time
        word_bl = [w.strip().lower() for w in cfg.get("word_blacklist", []) if w.strip()]
        if word_bl:
            self._candidates_pool = {
                cid: m for cid, m in self._candidates_pool.items()
                if not any(w in m.question.lower() for w in word_bl)
            }

        # Filter by budget: if max_order_usdc is set, exclude markets that can never be opened
        _max_order_usdc = float(cfg.get("max_order_usdc", 0) or 0)
        if _max_order_usdc > 0:
            self._candidates_pool = {
                cid: m for cid, m in self._candidates_pool.items()
                if m.min_order_cost <= _max_order_usdc
            }

        # Prune pool: keep top POOL_MAX_SIZE by score
        pool_sorted = sorted(self._candidates_pool.values(), key=lambda m: m.score, reverse=True)
        self._candidates_pool = {m.condition_id: m for m in pool_sorted[:POOL_MAX_SIZE]}

        slot_pct = cfg["slot_pct"]
        _max_positions = int(cfg.get("max_positions", 0) or 0)
        max_slots = _max_positions if _max_positions > 0 else max(1, int(100 / slot_pct))
        return pool_sorted[:max_slots]

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

        entered_ids = {p["condition_id"] for p in open_positions}
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

    # ── Enter a market ──────────────────────────────────────────────────────────

    async def _enter_market(self, market: ScoredMarket, slot_budget: float, depth: str) -> float:
        """Place a single-sided BUY order on the more expensive token.
        Uses the full slot_budget to maximize USDC locked → reward share.
        Returns total USDC spent."""

        if not market.tokens:
            return 0.0

        # Pick the more expensive token (price > 0.50) — more USDC locked per share
        valid = [t for t in market.tokens if 0 < float(t["price"]) < 1]
        if not valid:
            valid = market.tokens
        token = min(valid, key=lambda t: float(t["price"]))

        token_id = token["token_id"]
        token_mid = float(token["price"])
        if not (0 < token_mid < 1):
            token_mid = market.mid_price

        bids = market.orderbook_bids.get(token_id, [])
        target_price = calc_order_price(token_mid, market.rewards_max_spread, depth, bids=bids)

        # Guard: fetch live order book and ensure our bid is strictly below best ask.
        # post_only orders get immediately cancelled if bid >= best_ask (taker).
        live_ob = await client.get_order_book(token_id)
        if live_ob is not None:
            asks_raw = getattr(live_ob, "asks", None) or []
            ask_prices = sorted([float(a.price) for a in asks_raw if getattr(a, "price", None)])
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

        log.info(
            "Placing BUY %s @ %.3f size=%.0f cost≈$%.2f (budget=$%.2f) | %s",
            token.get("outcome", "?"), target_price, size, order_cost,
            slot_budget, market.question[:60],
        )

        resp = await client.place_limit(token_id, target_price, size, "BUY")
        if resp is None:
            log.warning("Failed to place order for %s", market.condition_id)
            return 0.0

        order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
        if not order_id:
            log.warning("No order_id in response for %s", market.condition_id)
            return 0.0

        await db.upsert_position({
            "order_id": order_id,
            "condition_id": market.condition_id,
            "market_question": market.question,
            "token_id": token_id,
            "side": "BUY",
            "price": target_price,
            "size": size,
            "status": "OPEN",
            "placed_at": datetime.now(timezone.utc).isoformat(),
            "filled_at": None,
            "reward_earned": 0,
        })
        return order_cost

    # ── Monitor open positions ──────────────────────────────────────────────────

    async def _monitor_positions(self, cfg: dict) -> None:
        # Recover any FILLED BUY positions whose SELL was never placed or was cancelled
        await self._recover_missing_sells()

        open_positions = await db.get_open_positions()
        if not open_positions:
            return

        depth = cfg["depth"]
        depth_changed = self._last_depth is not None and depth != self._last_depth
        if depth_changed:
            log.info("Depth changed %s → %s, forcing reposition of all open orders", self._last_depth, depth)
        self._last_depth = depth

        candidate_map = {m.condition_id: m for m in self.last_scan}

        for pos in open_positions:
            order_id = pos["order_id"]
            order = await client.get_order(order_id)
            if order is None:
                continue

            status = str(getattr(order, "status", "") or "").upper()

            if status in ("FILLED", "MATCHED"):
                log.warning("Order %s FILLED! Placing sell at front of ask queue.", order_id)
                await db.update_position_status(
                    order_id, "FILLED",
                    filled_at=datetime.now(timezone.utc).isoformat(),
                )
                # Get live order book to place sell at optimal ask price
                ob_for_sell = await client.get_order_book(pos["token_id"])
                live_mid_sell = mid_from_order_book(ob_for_sell)
                live_asks = _extract_asks(ob_for_sell)
                market_info = candidate_map.get(pos["condition_id"])
                spread_for_sell = market_info.rewards_max_spread if market_info else 0.04
                sell_mid = live_mid_sell or pos["price"]
                sell_price = calc_sell_order_price(sell_mid, spread_for_sell, live_asks)

                resp = await client.place_limit(pos["token_id"], sell_price, pos["size"], "SELL")
                if resp:
                    sell_order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
                    if sell_order_id:
                        await db.upsert_position({
                            "order_id": sell_order_id,
                            "condition_id": pos["condition_id"],
                            "market_question": pos.get("market_question", ""),
                            "token_id": pos["token_id"],
                            "side": "SELL",
                            "price": sell_price,
                            "size": pos["size"],
                            "status": "OPEN",
                            "placed_at": datetime.now(timezone.utc).isoformat(),
                            "filled_at": None,
                            "reward_earned": 0,
                        })
                        log.info(
                            "Sell order %s placed @ %.3f for filled BUY %s | %s",
                            sell_order_id, sell_price, order_id,
                            pos.get("market_question", "")[:50],
                        )
                continue

            if status in ("CANCELLED", "CANCELED"):
                await db.update_position_status(order_id, "CANCELLED")
                continue

            # Get live order book data (used by both BUY and SELL logic)
            order_book = await client.get_order_book(pos["token_id"])
            live_mid = mid_from_order_book(order_book)

            # ── Check 1: spread widened beyond threshold → exit position ─────
            ob_spread = _ob_spread_cents(order_book)
            max_ob_spread = float(cfg.get("max_ob_spread", 2.0))
            if ob_spread is not None and ob_spread > max_ob_spread:
                log.info(
                    "Cancel %s: spread %.1f¢ > max %.1f¢ (market degraded) | %s",
                    order_id, ob_spread, max_ob_spread, pos.get("market_question", "")[:45],
                )
                cancelled = await client.cancel_order(order_id)
                if cancelled:
                    await db.update_position_status(order_id, "CANCELLED")
                    # Remove from pool so bot doesn't re-enter until next rescan
                    self._candidates_pool.pop(pos["condition_id"], None)
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
                if price_drift or depth_changed:
                    log.info(
                        "Sell %s reposition %.4f → %.4f (drift=%s, forced=%s) | %s",
                        order_id, current_price, new_target,
                        f"{abs(current_price - new_target):.4f}", depth_changed,
                        pos.get("market_question", "")[:45],
                    )
                    cancelled = await client.cancel_order(order_id)
                    if cancelled:
                        await db.update_position_status(order_id, "CANCELLED")
                        resp = await client.place_limit(pos["token_id"], new_target, pos["size"], "SELL")
                        if resp:
                            new_order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
                            if new_order_id:
                                await db.upsert_position({
                                    **pos,
                                    "order_id": new_order_id,
                                    "price": new_target,
                                    "status": "OPEN",
                                    "placed_at": datetime.now(timezone.utc).isoformat(),
                                    "filled_at": None,
                                })

            else:
                # ── BUY: existing bid-side logic ─────────────────────────────
                live_bids = _extract_bids(order_book)

                if live_mid is not None:
                    new_target = calc_order_price(live_mid, spread, depth, bids=live_bids)
                elif market_info is not None:
                    new_target = calc_order_price(market_info.mid_price, spread, depth, bids=live_bids)
                else:
                    if not depth_changed:
                        log.debug("Skip reposition %s: no OB data and not in last_scan", order_id)
                        continue
                    new_target = calc_order_price(pos["price"] / 0.97, spread, depth)

                # ── Check 2: we're the best bid → step down to 2nd level ─────
                if live_bids:
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

                if price_drift or depth_changed:
                    log.info(
                        "Order %s reposition %.4f → %.4f (depth=%s, drift=%s, forced=%s)",
                        order_id, current_price, new_target, depth,
                        f"{abs(current_price - new_target):.4f}", depth_changed,
                    )
                    cancelled = await client.cancel_order(order_id)
                    if cancelled:
                        await db.update_position_status(order_id, "CANCELLED")
                        resp = await client.place_limit(pos["token_id"], new_target, pos["size"], pos["side"])
                        if resp:
                            new_order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
                            if new_order_id:
                                await db.upsert_position({
                                    **pos,
                                    "order_id": new_order_id,
                                    "price": new_target,
                                    "status": "OPEN",
                                    "placed_at": datetime.now(timezone.utc).isoformat(),
                                    "filled_at": None,
                                })

    # ── Recover missing sell orders ─────────────────────────────────────────────

    async def _recover_missing_sells(self) -> None:
        """Find FILLED BUY positions with no open SELL and place the missing sell orders."""
        orphans = await db.get_filled_buys_without_sell()
        if not orphans:
            return

        candidate_map = {m.condition_id: m for m in self.last_scan}

        for pos in orphans:
            log.info(
                "Recovering missing SELL for filled BUY %s | %s",
                pos["order_id"], pos.get("market_question", "")[:50],
            )
            ob = await client.get_order_book(pos["token_id"])
            if ob is None:
                # Orderbook gone → market resolved/closed; stop retrying
                log.info(
                    "Market resolved (no orderbook), marking BUY %s as CANCELLED | %s",
                    pos["order_id"], pos.get("market_question", "")[:50],
                )
                await db.update_position_status(pos["order_id"], "CANCELLED")
                continue

            live_mid = mid_from_order_book(ob)
            live_asks = _extract_asks(ob)
            market_info = candidate_map.get(pos["condition_id"])
            spread = market_info.rewards_max_spread if market_info else 0.04
            sell_mid = live_mid or pos["price"]
            sell_price = calc_sell_order_price(sell_mid, spread, live_asks)

            resp = await client.place_limit(pos["token_id"], sell_price, pos["size"], "SELL")
            if resp:
                sell_order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
                if sell_order_id:
                    await db.upsert_position({
                        "order_id": sell_order_id,
                        "condition_id": pos["condition_id"],
                        "market_question": pos.get("market_question", ""),
                        "token_id": pos["token_id"],
                        "side": "SELL",
                        "price": sell_price,
                        "size": pos["size"],
                        "status": "OPEN",
                        "placed_at": datetime.now(timezone.utc).isoformat(),
                        "filled_at": None,
                        "reward_earned": 0,
                    })
                    log.info(
                        "Recovered SELL %s @ %.3f for BUY %s | %s",
                        sell_order_id, sell_price, pos["order_id"],
                        pos.get("market_question", "")[:50],
                    )
            else:
                # Permanent failure (no tokens — market likely resolved with no position to sell)
                log.warning(
                    "Cannot place SELL for %s (no tokens or balance), marking BUY as CANCELLED | %s",
                    pos["order_id"], pos.get("market_question", "")[:50],
                )
                await db.update_position_status(pos["order_id"], "CANCELLED")

    # ── Config helpers ──────────────────────────────────────────────────────────

    async def _load_cfg(self) -> dict:
        from src.config import settings as s
        return {
            "slot_pct":            await db.get_setting("slot_pct",            s.slot_pct),
            "max_slots_per_market":await db.get_setting("max_slots_per_market",s.max_slots_per_market),
            "scan_interval_s":     await db.get_setting("scan_interval_s",     s.scan_interval_s),
            "min_daily_reward":    await db.get_setting("min_daily_reward",    s.min_daily_reward),
            "depth":               await db.get_setting("depth",               s.depth),
            "category_blacklist":  await db.get_setting("category_blacklist",  s.category_blacklist),
            "volatility_threshold":await db.get_setting("volatility_threshold",s.volatility_threshold),
            "min_spread":          await db.get_setting("min_spread",          s.min_spread),
            "max_ob_spread":       await db.get_setting("max_ob_spread",       s.max_ob_spread),
            "max_daily_trades":    await db.get_setting("max_daily_trades",    s.max_daily_trades),
            "monitor_interval_s":  await db.get_setting("monitor_interval_s",  s.monitor_interval_s),
            "max_order_usdc":      await db.get_setting("max_order_usdc",      s.max_order_usdc),
            "max_positions":       await db.get_setting("max_positions",       s.max_positions),
            "word_blacklist":      await db.get_setting("word_blacklist",      s.word_blacklist),
        }


bot = FarmingBot()
