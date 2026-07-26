"""Single-writer order lifecycle manager.

All bot order mutations pass through this module.  Market/trader/HTTP code may
decide *what* it wants, but only OrderManager is allowed to place, cancel, or
replace an order.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import defaultdict, deque
from datetime import datetime, timezone
from typing import Awaitable, Callable

from src import db
from src.pm_client import PlacementResult, client


log = logging.getLogger(__name__)
ExecutionCallback = Callable[[dict, str], Awaitable[None]]


class _SlidingWindowLimiter:
    def __init__(self, limit: int, window_s: float) -> None:
        self.limit = max(1, int(limit))
        self.window_s = max(0.1, float(window_s))
        self._events: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0] >= self.window_s:
                    self._events.popleft()
                if len(self._events) < self.limit:
                    self._events.append(now)
                    return
                wait_s = self.window_s - (now - self._events[0])
            await asyncio.sleep(max(0.01, wait_s))


class OrderManager:
    def __init__(self) -> None:
        self._token_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._order_locks: defaultdict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        self._execution_callback: ExecutionCallback | None = None
        self._exit_tasks: dict[str, asyncio.Task] = {}
        self._failure_count: defaultdict[tuple[str, str], int] = defaultdict(int)
        self._failure_cooldown_until: dict[tuple[str, str], float] = {}
        # Deliberately far below the exchange limit.  This is a circuit breaker,
        # not a throughput target for normal operation.
        self._place_burst_limiter = _SlidingWindowLimiter(5, 1.0)
        self._place_limiter = _SlidingWindowLimiter(120, 60.0)
        self._cancel_burst_limiter = _SlidingWindowLimiter(10, 1.0)
        self._cancel_limiter = _SlidingWindowLimiter(180, 60.0)

    def set_execution_callback(self, callback: ExecutionCallback) -> None:
        self._execution_callback = callback

    async def can_place(self, condition_id: str) -> bool:
        return await self.placement_cooldown_remaining(condition_id) <= 0

    async def placement_cooldown_remaining(self, condition_id: str) -> float:
        memory_delay = max(
            (
                max(0.0, until - time.monotonic())
                for (cid, _side), until in self._failure_cooldown_until.items()
                if cid == condition_id
            ),
            default=0.0,
        )
        persisted_delay = await db.get_condition_retry_delay(condition_id)
        return max(memory_delay, persisted_delay)

    async def _route_cooldown_remaining(
        self,
        condition_id: str,
        token_id: str,
        side: str,
    ) -> float:
        key = (condition_id, str(side).upper())
        memory_delay = max(
            0.0,
            self._failure_cooldown_until.get(key, 0.0) - time.monotonic(),
        )
        persisted_delay = await db.get_order_retry_delay(
            condition_id,
            token_id,
            side,
        )
        return max(memory_delay, persisted_delay)

    async def _record_failure(
        self,
        condition_id: str,
        token_id: str,
        error_code: str,
        side: str,
    ) -> float:
        side = str(side).upper()
        key = (condition_id, side)
        persisted_count = await db.get_order_consecutive_failures(
            condition_id,
            token_id,
            side,
        )
        count = max(self._failure_count[key], persisted_count) + 1
        self._failure_count[key] = count
        delays_by_code = {
            "not_enough_balance": (300.0, 900.0, 3600.0),
            "insufficient_allowance": (900.0, 3600.0),
            "post_only_would_cross": (5.0, 15.0, 60.0),
            "invalid_tick_size": (600.0, 1800.0),
            "invalid_min_size": (3600.0,),
            "market_not_ready": (1800.0, 3600.0),
            "rate_limited": (60.0, 300.0, 900.0),
            "fak_not_filled": (5.0, 15.0, 60.0),
            "insufficient_liquidity": (5.0, 15.0, 60.0),
            "duplicate_order": (300.0, 900.0),
            "auth_or_signature": (3600.0,),
            "invalid_nonce": (3600.0,),
            "invalid_request": (3600.0,),
        }
        if side == "SELL" and error_code in (
            "not_enough_balance",
            "insufficient_allowance",
        ):
            # Outcome-token settlement can trail the fill event briefly.
            delays = (2.0, 5.0, 15.0, 60.0)
        else:
            delays = delays_by_code.get(error_code, (120.0, 600.0, 3600.0))
        delay = delays[min(count - 1, len(delays) - 1)]
        self._failure_cooldown_until[key] = time.monotonic() + delay
        return delay

    def _record_success(self, condition_id: str, side: str) -> None:
        key = (condition_id, str(side).upper())
        self._failure_count.pop(key, None)
        self._failure_cooldown_until.pop(key, None)

    async def place_position(
        self,
        position: dict,
        *,
        post_only: bool = True,
        pending_status: str = "PENDING_PLACE",
        market_sell: bool = False,
        min_price: float | None = None,
    ) -> tuple[object | None, dict]:
        """Journal first, then place once; ambiguous responses require reconcile."""
        pos = dict(position)
        condition_id = str(pos.get("condition_id") or "")
        token_id = str(pos.get("token_id") or "")
        local_id = str(pos.get("order_id") or f"local-{uuid.uuid4().hex}")
        pos.update(
            {
                "order_id": local_id,
                "status": pending_status,
                "placed_at": pos.get("placed_at") or datetime.now(timezone.utc).isoformat(),
                "filled_at": None,
                "matched_size": float(pos.get("matched_size") or 0),
                "reward_earned": float(pos.get("reward_earned") or 0),
                "local_id": pos.get("local_id") or local_id,
                "source": pos.get("source") or "BOT",
            }
        )

        async with self._token_locks[token_id]:
            side = str(pos.get("side") or "").upper()
            cooldown = await self._route_cooldown_remaining(
                condition_id,
                token_id,
                side,
            )
            if cooldown > 0:
                pos["status"] = "SUPPRESSED"
                return PlacementResult(
                    outcome="rejected",
                    error_code="local_cooldown",
                    error_message=f"Placement suppressed for {cooldown:.0f}s",
                ), pos
            await db.upsert_position(pos)
            await db.begin_order_attempt(pos)
            await self._place_burst_limiter.acquire()
            await self._place_limiter.acquire()
            if market_sell:
                if side != "SELL" or min_price is None:
                    raise ValueError("market_sell requires SELL side and min_price")
                resp = await client.place_market_sell(
                    token_id,
                    float(pos["size"]),
                    min_price=float(min_price),
                )
            else:
                resp = await client.place_limit(
                    token_id,
                    float(pos["price"]),
                    float(pos["size"]),
                    side,
                    post_only=post_only,
                )

            outcome = _placement_outcome(resp)
            if outcome == "ambiguous":
                # The server may have accepted the order before the response was
                # lost.  Keep it reconcilable; never retry blindly.
                await db.update_position_status(local_id, "RECONCILE_REQUIRED")
                await db.finish_order_attempt(
                    pos,
                    outcome="AMBIGUOUS",
                    error_code=_result_error_code(resp),
                    error_message=_result_error_message(resp),
                )
                pos["status"] = "RECONCILE_REQUIRED"
                return resp, pos

            if outcome == "rejected":
                error_code = _result_error_code(resp) or "rejected"
                error_message = _result_error_message(resp)
                retry_delay = await self._record_failure(
                    condition_id,
                    token_id,
                    error_code,
                    side,
                )
                await db.finish_order_attempt(
                    pos,
                    outcome="REJECTED",
                    error_code=error_code,
                    error_message=error_message,
                    retry_delay_s=retry_delay,
                )
                # A definitive rejection is not an order and must not inflate
                # positions/history.  Its aggregate remains in order_attempts.
                await db.delete_position(local_id)
                pos["status"] = "FAILED"
                return resp, pos

            order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
            if not order_id:
                await db.update_position_status(local_id, "RECONCILE_REQUIRED")
                await db.finish_order_attempt(
                    pos,
                    outcome="AMBIGUOUS",
                    error_code="accepted_without_order_id",
                    error_message="Placement response had no order id",
                )
                pos["status"] = "RECONCILE_REQUIRED"
                return resp, pos

            response_status = str(getattr(resp, "status", "") or "").upper()
            local_status = "OPEN" if response_status in ("", "LIVE", "OPEN") else "RECONCILE_REQUIRED"
            await db.replace_position_order_id(local_id, order_id, status=local_status)
            pos.update({"order_id": order_id, "status": local_status})
            await db.finish_order_attempt(
                pos,
                outcome="ACCEPTED",
                remote_order_id=order_id,
            )
            self._record_success(condition_id, side)

            # MATCHED is not proof that the full requested size executed.
            # Fetch the cumulative amount when available; WS remains authoritative.
            if response_status == "MATCHED":
                remote = await client.get_order(order_id)
                if remote is not None:
                    matched = _as_float(getattr(remote, "size_matched", 0))
                    if matched > 0:
                        updated = await db.record_cumulative_match(
                            order_id,
                            matched,
                            source="PLACE_RESPONSE",
                        )
                        if updated is not None:
                            pos = updated
                            asyncio.create_task(
                                self.handle_order_update(
                                    order_id=order_id,
                                    matched_size=matched,
                                    status=str(getattr(remote, "status", "") or response_status),
                                    order_type="PLACE_RESPONSE",
                                )
                            )
            return resp, pos

    async def cancel_order(self, order_id: str, *, reason: str = "") -> bool:
        pos = await db.get_position(order_id)
        if str(order_id).startswith("local-"):
            # A local id represents a placement whose POST response was lost;
            # it is never a valid CLOB order id. Reconciliation must either
            # adopt the matching remote order or retire the verified ghost.
            log.warning(
                "Refusing remote cancel for local journal id=%s; reconcile first",
                order_id,
            )
            return False
        token_id = str((pos or {}).get("token_id") or order_id)
        async with self._token_locks[token_id]:
            async with self._order_locks[order_id]:
                cancelled = await self._cancel_locked(pos, order_id, reason=reason)
                if cancelled and pos is not None:
                    delay = _cancel_suppression_delay(reason)
                    if delay > 0:
                        condition_id = str(pos.get("condition_id") or "")
                        key = (
                            condition_id,
                            str(pos.get("side") or "").upper(),
                        )
                        self._failure_cooldown_until[key] = max(
                            self._failure_cooldown_until.get(key, 0.0),
                            time.monotonic() + delay,
                        )
                        await db.set_condition_retry_delay(
                            condition_id,
                            delay,
                            side=str(pos.get("side") or "").upper(),
                        )
                return cancelled

    async def cancel_all(self, *, reason: str = "cancel_all") -> bool:
        """Bulk cancel, then reconcile every locally working order."""
        ok = await client.cancel_all()
        if not ok:
            return False
        for pos in await db.get_reconcilable_positions():
            order_id = str(pos.get("order_id") or "")
            remote = await client.get_order(order_id)
            matched = _as_float(getattr(remote, "size_matched", 0)) if remote else float(pos.get("matched_size") or 0)
            if matched > 0:
                updated = await db.record_cumulative_match(
                    order_id,
                    matched,
                    source="CANCEL_ALL_RECONCILE",
                ) or pos
                if str(updated.get("side") or "").upper() == "BUY":
                    await db.update_position_status(order_id, "EXIT_REQUIRED")
                    updated["status"] = "EXIT_REQUIRED"
                    self._schedule_execution_callback(updated, reason)
                else:
                    await db.update_position_status(order_id, "CANCELLED")
                    self._schedule_execution_callback(updated, reason)
            else:
                await db.update_position_status(order_id, "CANCELLED")
        return True

    async def _cancel_locked(self, pos: dict | None, order_id: str, *, reason: str) -> bool:
        if pos is not None:
            await db.update_position_status(order_id, "CANCEL_PENDING")

        await self._cancel_burst_limiter.acquire()
        await self._cancel_limiter.acquire()
        cancelled = await client.cancel_order(order_id)
        remote = await client.get_order(order_id)
        if pos is not None and remote is not None:
            matched = _as_float(getattr(remote, "size_matched", 0))
            if matched > 0:
                pos = await db.record_cumulative_match(
                    order_id,
                    matched,
                    source="CANCEL_RECONCILE",
                ) or pos

        if not cancelled:
            if pos is not None:
                pos = await db.get_position(order_id) or pos
                matched = float(pos.get("matched_size") or 0)
                side = str(pos.get("side") or "").upper()
                if matched > 0 and side == "BUY":
                    await db.update_position_status(order_id, "EXIT_REQUIRED")
                    pos["status"] = "EXIT_REQUIRED"
                    self._schedule_execution_callback(pos, reason or "cancel_not_confirmed_after_fill")
                elif matched > 0 and side == "SELL":
                    await db.update_position_status(order_id, "RECONCILE_REQUIRED")
                    self._schedule_execution_callback(pos, reason or "sell_cancel_not_confirmed")
                else:
                    await db.update_position_status(order_id, "RECONCILE_REQUIRED")
            return False

        if pos is None:
            return True

        pos = await db.get_position(order_id) or pos
        matched = float(pos.get("matched_size") or 0)
        side = str(pos.get("side") or "").upper()
        if side == "BUY" and matched > 0:
            await db.update_position_status(order_id, "EXIT_REQUIRED")
            pos["status"] = "EXIT_REQUIRED"
            self._schedule_execution_callback(pos, reason or "partial_buy_cancelled")
        else:
            await db.update_position_status(order_id, "CANCELLED")
            pos["status"] = "CANCELLED"
            if side == "SELL" and matched > 0:
                self._schedule_execution_callback(pos, reason or "partial_sell_cancelled")
        return True

    async def replace_order(
        self,
        pos: dict,
        price: float,
        size: float,
        *,
        side: str | None = None,
    ) -> bool:
        """Cancel and replace under a per-token lock.

        A BUY that acquired any shares is never replaced.  Its remainder is
        cancelled and the acquired inventory is sent to the exit workflow.
        """
        order_id = str(pos.get("order_id") or "")
        token_id = str(pos.get("token_id") or "")
        async with self._token_locks[token_id]:
            async with self._order_locks[order_id]:
                fresh = await db.get_position(order_id) or pos
                if float(fresh.get("matched_size") or 0) > 0:
                    await self._cancel_locked(fresh, order_id, reason="replace_after_partial")
                    return False

                cancelled = await self._cancel_locked(fresh, order_id, reason="replace")
                if not cancelled:
                    return False

                replacement = {
                    **fresh,
                    "order_id": f"local-{uuid.uuid4().hex}",
                    "price": float(price),
                    "size": float(size),
                    "side": str(side or fresh["side"]).upper(),
                    "status": "PENDING_PLACE",
                    "placed_at": datetime.now(timezone.utc).isoformat(),
                    "filled_at": None,
                    "matched_size": 0.0,
                    "local_id": None,
                }
                replacement["local_id"] = replacement["order_id"]

                # We already hold the token lock, so place inline rather than
                # recursively acquiring the same non-reentrant asyncio.Lock.
                await db.upsert_position(replacement)
                await db.begin_order_attempt(replacement)
                await self._place_burst_limiter.acquire()
                await self._place_limiter.acquire()
                resp = await client.place_limit(
                    token_id,
                    float(price),
                    float(size),
                    replacement["side"],
                )
                local_id = replacement["order_id"]
                outcome = _placement_outcome(resp)
                if outcome == "ambiguous":
                    await db.update_position_status(local_id, "RECONCILE_REQUIRED")
                    await db.finish_order_attempt(
                        replacement,
                        outcome="AMBIGUOUS",
                        error_code=_result_error_code(resp),
                        error_message=_result_error_message(resp),
                    )
                    return False
                if outcome == "rejected":
                    condition_id = str(fresh.get("condition_id") or "")
                    error_code = _result_error_code(resp) or "rejected"
                    delay = await self._record_failure(
                        condition_id,
                        token_id,
                        error_code,
                        replacement["side"],
                    )
                    await db.finish_order_attempt(
                        replacement,
                        outcome="REJECTED",
                        error_code=error_code,
                        error_message=_result_error_message(resp),
                        retry_delay_s=delay,
                    )
                    await db.delete_position(local_id)
                    return False
                new_order_id = str(getattr(resp, "order_id", "") or getattr(resp, "id", ""))
                if not new_order_id:
                    await db.update_position_status(local_id, "RECONCILE_REQUIRED")
                    await db.finish_order_attempt(
                        replacement,
                        outcome="AMBIGUOUS",
                        error_code="accepted_without_order_id",
                        error_message="Placement response had no order id",
                    )
                    return False
                response_status = str(getattr(resp, "status", "") or "").upper()
                new_status = "OPEN" if response_status in ("", "LIVE", "OPEN") else "RECONCILE_REQUIRED"
                await db.replace_position_order_id(local_id, new_order_id, status=new_status)
                await db.finish_order_attempt(
                    replacement,
                    outcome="ACCEPTED",
                    remote_order_id=new_order_id,
                )
                self._record_success(
                    str(fresh.get("condition_id") or ""),
                    replacement["side"],
                )
                if response_status == "MATCHED":
                    remote = await client.get_order(new_order_id)
                    matched = _as_float(getattr(remote, "size_matched", 0)) if remote else 0.0
                    if matched > 0:
                        asyncio.create_task(
                            self.handle_order_update(
                                order_id=new_order_id,
                                matched_size=matched,
                                status=str(getattr(remote, "status", "") or response_status),
                                order_type="REPLACE_RESPONSE",
                            )
                        )
                return True

    async def handle_order_update(
        self,
        *,
        order_id: str,
        matched_size: float,
        status: str = "",
        order_type: str = "",
    ) -> dict | None:
        """Apply a WS cumulative update and rescue the first partial BUY."""
        pos = await db.get_position(order_id)
        if pos is None:
            return None
        token_id = str(pos.get("token_id") or "")

        callback_pos: dict | None = None
        callback_reason = ""
        async with self._token_locks[token_id]:
            async with self._order_locks[order_id]:
                updated = await db.record_cumulative_match(
                    order_id,
                    matched_size,
                    source="USER_WS",
                )
                if updated is None:
                    return None

                side = str(updated.get("side") or "").upper()
                total = float(updated.get("size") or 0)
                matched = float(updated.get("matched_size") or 0)
                is_full = total > 0 and matched >= total - 0.0001
                is_cancel = (
                    str(order_type).upper() == "CANCELLATION"
                    or str(status).upper() in ("CANCELED", "CANCELLED")
                )

                if side == "BUY" and matched > 0:
                    if not is_full and not is_cancel:
                        await self._cancel_locked(updated, order_id, reason="partial_buy_ws")
                        updated = await db.get_position(order_id) or updated
                    else:
                        await db.update_position_status(order_id, "EXIT_REQUIRED")
                        updated["status"] = "EXIT_REQUIRED"
                    callback_pos = updated
                    callback_reason = "partial_buy" if not is_full else "filled_buy"
                elif side == "SELL" and matched > 0:
                    if is_full:
                        await db.update_position_status(
                            order_id,
                            "FILLED",
                            filled_at=datetime.now(timezone.utc).isoformat(),
                        )
                        updated["status"] = "FILLED"
                    elif is_cancel:
                        await db.update_position_status(order_id, "CANCELLED")
                        updated["status"] = "CANCELLED"
                    else:
                        # A marketable SELL that only partially executes must
                        # not be called FILLED.  Cancel the residual and retry it.
                        await self._cancel_locked(updated, order_id, reason="partial_sell_ws")
                        updated = await db.get_position(order_id) or updated
                    callback_pos = updated
                    callback_reason = "partial_sell"
                elif is_cancel:
                    await db.update_position_status(order_id, "CANCELLED")
                    updated["status"] = "CANCELLED"

        if callback_pos is not None:
            self._schedule_execution_callback(callback_pos, callback_reason)
        return callback_pos or updated

    def _schedule_execution_callback(self, pos: dict, reason: str) -> None:
        if self._execution_callback is None:
            return
        key = str(pos.get("parent_order_id") or pos.get("order_id") or "")
        running = self._exit_tasks.get(key)
        if running is not None and not running.done():
            return

        async def runner() -> None:
            try:
                await self._execution_callback(pos, reason)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Execution callback failed order=%s reason=%s", key, reason)
            finally:
                self._exit_tasks.pop(key, None)

        self._exit_tasks[key] = asyncio.create_task(runner())


def _as_float(value) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def _placement_outcome(result: object | None) -> str:
    if result is None:
        return "ambiguous"
    outcome = str(getattr(result, "outcome", "") or "").lower()
    if outcome in ("accepted", "rejected", "ambiguous"):
        return outcome
    return "accepted" if getattr(result, "ok", True) else "rejected"


def _result_error_code(result: object | None) -> str:
    return str(
        getattr(result, "error_code", "")
        or getattr(result, "code", "")
        or ""
    )


def _result_error_message(result: object | None) -> str:
    return str(
        getattr(result, "error_message", "")
        or getattr(result, "message", "")
        or ""
    )


def _cancel_suppression_delay(reason: str) -> float:
    normalized = str(reason or "").lower()
    policies = {
        "level_share": 300.0,
        "target_level_too_small": 300.0,
        "thin_bid_depth": 600.0,
        "front_run": 60.0,
        "unprotected_best_bid": 60.0,
        "no_safe_step_level": 60.0,
        "rebalance": 300.0,
    }
    for token, delay in policies.items():
        if token in normalized:
            return delay
    if "volatility" in normalized or "price changes/day" in normalized:
        return 1800.0
    return 0.0


order_manager = OrderManager()
