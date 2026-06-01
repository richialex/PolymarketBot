"""Thin async-friendly wrapper around the Polymarket py-sdk."""
from __future__ import annotations

import asyncio
import logging
from decimal import Decimal
from typing import Any

from polymarket import (
    PublicClient, SecureClient,
    CurrentReward, MarketReward,
    OrderBook,
    PriceHistoryPoint,
    OpenOrder,
    OrderResponse,
    Market,
)

from src.config import settings

log = logging.getLogger(__name__)


_CONN_ERRORS = ("connectionterminated", "connection terminated", "remoteerror",
                "connectionreset", "broken pipe", "eof occurred")


def _is_conn_error(e: Exception) -> bool:
    return any(kw in str(e).lower() for kw in _CONN_ERRORS)


class PMClient:
    def __init__(self) -> None:
        self._pub: PublicClient | None = None
        self._sec: SecureClient | None = None

    def _reset(self) -> None:
        """Drop cached clients so next call creates fresh connections."""
        self._pub = None
        self._sec = None

    def _public(self) -> PublicClient:
        if self._pub is None:
            self._pub = PublicClient()
        return self._pub

    def _secure(self) -> SecureClient:
        if self._sec is None:
            self._sec = SecureClient.create(
                private_key=settings.private_key,
                wallet=settings.wallet_address,
            )
        return self._sec

    # ── Rewards ────────────────────────────────────────────────────────────────

    async def get_all_rewards(self) -> list[CurrentReward]:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                paginator = await loop.run_in_executor(
                    None, lambda: self._public().list_current_rewards()
                )
                return list(paginator.items())
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("get_all_rewards connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("get_all_rewards: %s", e)
                return []

    async def get_market_reward(self, condition_id: str) -> MarketReward | None:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                paginator = await loop.run_in_executor(
                    None,
                    lambda: self._public().list_market_rewards(condition_id=condition_id),
                )
                for item in paginator.items():
                    return item
                return None
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("get_market_reward connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("get_market_reward %s: %s", condition_id, e)
                return None

    # ── Markets ────────────────────────────────────────────────────────────────

    async def get_market_by_slug(self, slug: str) -> Market | None:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                return await loop.run_in_executor(
                    None,
                    lambda: self._public().get_market(slug=slug),
                )
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("get_market_by_slug connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("get_market slug=%s: %s", slug, e)
                return None

    async def get_balance(self) -> Decimal:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                bal = await loop.run_in_executor(
                    None,
                    lambda: self._secure().get_balance_allowance(asset_type="COLLATERAL"),
                )
                return Decimal(str(bal.balance)) / Decimal("1000000")
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("get_balance connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("get_balance: %s", e)
                return Decimal("0")

    # ── Order book ─────────────────────────────────────────────────────────────

    async def get_order_book(self, token_id: str) -> OrderBook | None:
        loop = asyncio.get_event_loop()
        for attempt in range(3):
            try:
                return await loop.run_in_executor(
                    None,
                    lambda tid=token_id: self._public().get_order_book(token_id=tid),
                )
            except Exception as e:
                if _is_conn_error(e) and attempt < 2:
                    log.warning("get_order_book connection reset, retrying (attempt %d): %s", attempt + 1, e)
                    self._reset()
                    await asyncio.sleep(0.5)
                    continue
                log.warning("get_order_book %s: %s", token_id, e)
                return None

    # ── Price history ──────────────────────────────────────────────────────────

    async def get_price_history(
        self, token_id: str, interval: str = "1d"
    ) -> list[PriceHistoryPoint]:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                result = await loop.run_in_executor(
                    None,
                    lambda: self._public().get_price_history(
                        token_id=token_id, interval=interval
                    ),
                )
                return list(result)
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("get_price_history connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("get_price_history %s: %s", token_id, e)
                return []

    # ── Orders ─────────────────────────────────────────────────────────────────

    async def place_limit(
        self,
        token_id: str,
        price: float,
        size: float,
        side: str,
    ) -> OrderResponse | None:
        loop = asyncio.get_event_loop()
        for attempt in range(4):
            _p = price
            try:
                return await loop.run_in_executor(
                    None,
                    lambda p=_p: self._secure().place_limit_order(
                        token_id=token_id,
                        price=Decimal(str(p)),
                        size=Decimal(str(size)),
                        side=side,
                        post_only=True,
                    ),
                )
            except Exception as e:
                err = str(e)
                if "tick size" in err and attempt == 0:
                    price = round(price, 2)
                    log.info("place_limit: tick-size error, rounding %.4f → %.2f | %s", _p, price, token_id[:20])
                    continue
                if _is_conn_error(e) and attempt < 3:
                    log.warning("place_limit connection reset, retrying (attempt %d): %s", attempt + 1, e)
                    self._reset()
                    await asyncio.sleep(1)
                    continue
                log.error("place_limit %s: %s", token_id, e)
                return None

    async def cancel_order(self, order_id: str) -> bool:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                await loop.run_in_executor(
                    None,
                    lambda: self._secure().cancel_order(order_id=order_id),
                )
                return True
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("cancel_order connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.error("cancel_order %s: %s", order_id, e)
                return False

    async def cancel_all(self) -> bool:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                await loop.run_in_executor(None, lambda: self._secure().cancel_all())
                return True
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("cancel_all connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.error("cancel_all: %s", e)
                return False

    async def get_order(self, order_id: str) -> OpenOrder | None:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                return await loop.run_in_executor(
                    None,
                    lambda: self._secure().get_order(order_id=order_id),
                )
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("get_order connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("get_order %s: %s", order_id, e)
                return None

    async def list_open_orders(self) -> list[OpenOrder]:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                paginator = await loop.run_in_executor(
                    None,
                    lambda: self._secure().list_open_orders(),
                )
                return list(paginator.items())
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("list_open_orders connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("list_open_orders: %s", e)
                return []

    async def get_locked_usdc(self) -> float:
        """Return USDC actually locked in open BUY orders on Polymarket.
        SELL orders lock conditional tokens, not USDC — excluded from sum."""
        orders = await self.list_open_orders()
        total = 0.0
        for o in orders:
            try:
                side = str(getattr(o, "side", "") or "").upper()
                if side == "SELL":
                    continue  # SELL locks tokens, not USDC
                price = float(getattr(o, "price", 0) or 0)
                original = float(getattr(o, "original_size", 0) or 0)
                matched = float(getattr(o, "size_matched", 0) or 0)
                remaining = max(0.0, original - matched)
                total += price * remaining
            except (TypeError, ValueError):
                pass
        return round(total, 4)


client = PMClient()
