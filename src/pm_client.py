"""Thin async-friendly wrapper around the Polymarket py-sdk."""
from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Literal

import httpx
from polymarket.errors import (
    InsufficientAllowanceError,
    InsufficientLiquidityError,
    RateLimitError,
    RequestRejectedError,
    SigningError,
    TransportError,
    UnexpectedResponseError,
    UserInputError,
)
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
                "connectionreset", "broken pipe", "eof occurred",
                "server disconnected", "handshake operation timed out",
                "timed out", "timeout")


@dataclass(frozen=True)
class PlacementResult:
    """Normalized result of the non-idempotent POST /order call."""

    outcome: Literal["accepted", "rejected", "ambiguous"]
    order_id: str = ""
    status: str = ""
    error_code: str = ""
    error_message: str = ""
    attempts: int = 1

    @property
    def ok(self) -> bool:
        return self.outcome == "accepted"

    @property
    def ambiguous(self) -> bool:
        return self.outcome == "ambiguous"


def _is_conn_error(e: Exception) -> bool:
    return isinstance(e, (TransportError, httpx.TransportError, TimeoutError)) or any(
        kw in str(e).lower() for kw in _CONN_ERRORS
    )


def _classify_order_error(error: Exception) -> tuple[str, str, bool]:
    """Return error_code, message, and whether the POST outcome is ambiguous."""
    message = str(error) or error.__class__.__name__
    lowered = message.lower()

    if _is_conn_error(error) or isinstance(error, UnexpectedResponseError):
        return "transport_ambiguous", message, True
    if isinstance(error, RateLimitError) or "rate limit" in lowered or "too many requests" in lowered:
        return "rate_limited", message, False
    if isinstance(error, InsufficientLiquidityError):
        return "insufficient_liquidity", message, False
    if "not enough balance" in lowered or "allowance is not enough" in lowered:
        return "not_enough_balance", message, False
    if isinstance(error, InsufficientAllowanceError):
        return "insufficient_allowance", message, False
    if "post-only" in lowered and ("cross" in lowered or "match" in lowered):
        return "post_only_would_cross", message, False
    if "tick size" in lowered or "minimum tick" in lowered:
        return "invalid_tick_size", message, False
    if "minimum" in lowered and "size" in lowered:
        return "invalid_min_size", message, False
    if "market is not yet ready" in lowered or "market_not_ready" in lowered:
        return "market_not_ready", message, False
    if "duplicated" in lowered or "duplicate" in lowered:
        return "duplicate_order", message, False
    if "invalid nonce" in lowered:
        return "invalid_nonce", message, False
    if "invalid signature" in lowered or isinstance(error, SigningError):
        return "auth_or_signature", message, False
    if isinstance(error, UserInputError):
        return "invalid_request", message, False
    if isinstance(error, RequestRejectedError):
        return f"http_{error.status}", message, False
    # Unknown runtime failures after starting a POST are treated
    # conservatively: the exchange may have accepted the order.
    return "unknown_ambiguous", message, True


def _paginator_items(paginator: Any) -> list[Any]:
    iter_items = getattr(paginator, "iter_items", None)
    if callable(iter_items):
        return list(iter_items())

    items = getattr(paginator, "items", None)
    if callable(items):
        return list(items())

    out: list[Any] = []
    for entry in paginator:
        page_items = getattr(entry, "items", None)
        if page_items is not None and not callable(page_items):
            out.extend(page_items)
        else:
            out.append(entry)
    return out


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

    def ws_auth(self) -> dict[str, str]:
        creds = self._secure().credentials
        return {
            "apiKey": creds.key,
            "secret": creds.secret,
            "passphrase": creds.passphrase,
        }

    # ── Rewards ────────────────────────────────────────────────────────────────

    async def get_all_rewards(self) -> list[CurrentReward]:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                paginator = await loop.run_in_executor(
                    None, lambda: self._public().list_current_rewards()
                )
                return _paginator_items(paginator)
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
                for item in _paginator_items(paginator):
                    return item
                return None
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("get_market_reward connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("get_market_reward %s: %s", condition_id, e)
                return None

    async def get_reward_markets_multi(
        self,
        *,
        page_size: int = 500,
        max_pages: int = 20,
        order_by: str = "rate_per_day",
        position: str = "DESC",
    ) -> list[dict[str, Any]]:
        """Fetch active reward markets from the raw multi endpoint.

        The installed SDK does not expose /rewards/markets/multi yet, so keep
        this as a narrow raw HTTP wrapper until the SDK grows a typed method.
        """
        page_size = min(500, max(1, int(page_size)))
        params: dict[str, Any] = {
            "page_size": page_size,
            "order_by": order_by,
            "position": position,
        }
        out: list[dict[str, Any]] = []
        cursor: str | None = None
        seen_cursors: set[str] = set()
        seen_conditions: set[str] = set()
        async with httpx.AsyncClient(timeout=20, trust_env=True) as http:
            for page in range(max(1, int(max_pages))):
                if cursor:
                    if cursor in seen_cursors:
                        log.warning("get_reward_markets_multi repeated cursor=%s; stopping pagination", cursor)
                        break
                    seen_cursors.add(cursor)
                    params["next_cursor"] = cursor
                try:
                    resp = await http.get(
                        "https://clob.polymarket.com/rewards/markets/multi",
                        params=params,
                    )
                    resp.raise_for_status()
                    payload = resp.json()
                except Exception as e:
                    log.warning("get_reward_markets_multi page=%d cursor=%s: %s", page + 1, cursor or "-", e)
                    break

                data = payload.get("data") if isinstance(payload, dict) else None
                if isinstance(data, list):
                    for item in data:
                        if not isinstance(item, dict):
                            continue
                        condition_id = str(item.get("condition_id") or "")
                        if condition_id and condition_id in seen_conditions:
                            continue
                        if condition_id:
                            seen_conditions.add(condition_id)
                        out.append(item)

                next_cursor = str(payload.get("next_cursor") or "") if isinstance(payload, dict) else ""
                if not next_cursor or next_cursor == "LTE=":
                    break
                cursor = next_cursor
        return out

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
        balance, _reachable = await self.get_balance_status()
        return balance

    async def get_balance_status(self) -> tuple[Decimal, bool]:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                bal = await loop.run_in_executor(
                    None,
                    lambda: self._secure().get_balance_allowance(asset_type="COLLATERAL"),
                )
                return Decimal(str(bal.balance)) / Decimal("1000000"), True
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("get_balance connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("get_balance: %s", e)
                return Decimal("0"), False

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
        post_only: bool = True,
    ) -> PlacementResult:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            _p = price
            try:
                response: OrderResponse = await loop.run_in_executor(
                    None,
                    lambda p=_p: self._secure().place_limit_order(
                        token_id=token_id,
                        price=Decimal(str(p)),
                        size=Decimal(str(size)),
                        side=side,
                        post_only=post_only,
                        builder_code=settings.builder_code.strip() or None,
                    ),
                )
                if getattr(response, "ok", False):
                    return PlacementResult(
                        outcome="accepted",
                        order_id=str(getattr(response, "order_id", "") or ""),
                        status=str(getattr(response, "status", "") or ""),
                        attempts=attempt + 1,
                    )
                code = str(getattr(response, "code", "") or "rejected")
                message = str(getattr(response, "message", "") or code)
                if code in ("invalid_tick_size", "invalid_tick") and attempt == 0:
                    price = round(price, 2)
                    log.info(
                        "place_limit: tick-size rejection, rounding %.4f → %.2f | %s",
                        _p,
                        price,
                        token_id[:20],
                    )
                    continue
                return PlacementResult(
                    outcome="rejected",
                    error_code=code,
                    error_message=message,
                    attempts=attempt + 1,
                )
            except Exception as e:
                code, message, ambiguous = _classify_order_error(e)
                if code == "invalid_tick_size" and attempt == 0:
                    price = round(price, 2)
                    log.info(
                        "place_limit: tick-size error, rounding %.4f → %.2f | %s",
                        _p,
                        price,
                        token_id[:20],
                    )
                    continue
                if ambiguous:
                    # POST /order is not idempotent.  The exchange may have
                    # accepted it before the response was lost; reconciliation
                    # must decide whether it exists instead of posting again.
                    log.warning(
                        "place_limit ambiguous error code=%s; not retrying: %s",
                        code,
                        message,
                    )
                    self._reset()
                    return PlacementResult(
                        outcome="ambiguous",
                        error_code=code,
                        error_message=message,
                        attempts=attempt + 1,
                    )
                log.error("place_limit rejected token=%s code=%s: %s", token_id, code, message)
                return PlacementResult(
                    outcome="rejected",
                    error_code=code,
                    error_message=message,
                    attempts=attempt + 1,
                )
        return PlacementResult(
            outcome="rejected",
            error_code="invalid_tick_size",
            error_message="Tick-size correction did not produce a valid order",
            attempts=2,
        )

    async def place_market_sell(
        self,
        token_id: str,
        shares: float,
        *,
        min_price: float,
    ) -> PlacementResult:
        """Place a SELL FAK for dust that cannot satisfy the limit-order minimum.

        ``min_price`` caps slippage at the observed best bid.  A market POST is
        non-idempotent, so an ambiguous response is reconciled rather than
        retried blindly.
        """
        loop = asyncio.get_event_loop()
        try:
            response: OrderResponse = await loop.run_in_executor(
                None,
                lambda: self._secure().place_market_order(
                    token_id=token_id,
                    side="SELL",
                    shares=Decimal(str(shares)),
                    min_price=Decimal(str(min_price)),
                    order_type="FAK",
                    builder_code=settings.builder_code.strip() or None,
                ),
            )
            if getattr(response, "ok", False):
                return PlacementResult(
                    outcome="accepted",
                    order_id=str(getattr(response, "order_id", "") or ""),
                    status=str(getattr(response, "status", "") or ""),
                )
            code = str(getattr(response, "code", "") or "rejected")
            return PlacementResult(
                outcome="rejected",
                error_code=code,
                error_message=str(getattr(response, "message", "") or code),
            )
        except Exception as e:
            code, message, ambiguous = _classify_order_error(e)
            if ambiguous:
                log.warning(
                    "place_market_sell ambiguous error code=%s; not retrying: %s",
                    code,
                    message,
                )
                self._reset()
                return PlacementResult(
                    outcome="ambiguous",
                    error_code=code,
                    error_message=message,
                )
            log.error(
                "place_market_sell rejected token=%s code=%s: %s",
                token_id,
                code,
                message,
            )
            return PlacementResult(
                outcome="rejected",
                error_code=code,
                error_message=message,
            )

    async def cancel_order(self, order_id: str) -> bool:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                response = await loop.run_in_executor(
                    None,
                    lambda: self._secure().cancel_order(order_id=order_id),
                )
                canceled = getattr(response, "canceled", None)
                not_canceled = getattr(response, "not_canceled", None)
                if isinstance(response, dict):
                    canceled = response.get("canceled", canceled)
                    not_canceled = response.get("not_canceled", not_canceled)
                if canceled is not None:
                    ok = str(order_id) in {str(item) for item in (canceled or [])}
                    if not ok:
                        log.warning(
                            "cancel_order not confirmed id=%s reason=%s",
                            order_id,
                            (not_canceled or {}).get(order_id, "unknown")
                            if isinstance(not_canceled, dict)
                            else not_canceled,
                        )
                    return ok
                # Compatibility with the beta SDK's older empty response.
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
        orders, _reachable = await self.list_open_orders_status()
        return orders

    async def list_open_orders_status(self) -> tuple[list[OpenOrder], bool]:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                paginator = await loop.run_in_executor(
                    None,
                    lambda: self._secure().list_open_orders(),
                )
                return _paginator_items(paginator), True
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("list_open_orders connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("list_open_orders: %s", e)
                return [], False

    async def list_positions(self) -> list[Any]:
        positions, _reachable = await self.list_positions_status()
        return positions

    async def list_positions_status(self) -> tuple[list[Any], bool]:
        loop = asyncio.get_event_loop()
        for attempt in range(2):
            try:
                paginator = await loop.run_in_executor(
                    None,
                    lambda: self._secure().list_positions(size_threshold=0.0001, page_size=100),
                )
                return _paginator_items(paginator), True
            except Exception as e:
                if _is_conn_error(e) and attempt == 0:
                    log.warning("list_positions connection reset, retrying: %s", e)
                    self._reset()
                    continue
                log.warning("list_positions: %s", e)
                return [], False

    async def get_token_position_size(self, token_id: str) -> float:
        size, _reachable = await self.get_token_position_size_status(token_id)
        return size

    async def get_token_position_size_status(self, token_id: str) -> tuple[float, bool]:
        positions, reachable = await self.list_positions_status()
        total = 0.0
        for pos in positions:
            if str(getattr(pos, "token_id", "") or "") != str(token_id):
                continue
            try:
                total += float(getattr(pos, "size", 0) or 0)
            except (TypeError, ValueError):
                pass
        return total, reachable

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
