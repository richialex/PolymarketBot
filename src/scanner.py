"""Market scanning, filtering, and scoring logic."""
from __future__ import annotations

import asyncio
import logging
import random as _random
import statistics
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Any

from src.pm_client import client

log = logging.getLogger(__name__)

BLACKLISTED_CATEGORIES: set[str] = set()
QUESTION_BLACKLIST: set[str] = {
    "democratic party", "republican party", "trump", "biden", "harris",
    "election", "popular vote", "congress", "senate", "house of representatives",
    "russia", "russian",
}
PREFERRED_CATEGORIES = {"gaming", "crypto", "sports", "esports", "technology", "token"}
FARM_MODES = ("cheap", "expensive", "both", "auto")
BOTH_SCAN_MODES = ("cheap", "strict")

# Politics-related tag variants from Polymarket API (supplement the user's category_blacklist)
POLITICS_TAG_VARIANTS: set[str] = {
    "politics", "us-politics", "us-elections", "elections", "political",
    "democrat", "republican", "government", "geopolitics",
}

# Selection quotas per scan:
#   HIGH_RATE  — top markets by daily_rate (competitive, high absolute reward)
#   MID_RATE   — moderate daily_rate (decent reward, some competition)
#   GEM        — low daily_rate (1-3/day), random sample — find empty-zone gems
HIGH_RATE_CUTOFF = 10.0   # $/day — "big" market threshold
MID_RATE_CUTOFF  = 3.0    # $/day — lower bound for mid tier
HIGH_RATE_QUOTA  = 30
MID_RATE_QUOTA   = 20
GEM_QUOTA        = 50     # random sample from 1.0–3.0/day — covers ~2.3% per scan


@dataclass
class ScoredMarket:
    condition_id: str
    question: str
    market_slug: str
    event_slug: str
    total_daily_rate: float
    rewards_min_size: float
    rewards_max_spread: float
    market_competitiveness: float
    price_volatility: float
    end_date: str | None
    score: float
    tokens: list[dict] = field(default_factory=list)
    category: str = ""
    mid_price: float = 0.5
    orderbook_bids: dict[str, list[float]] = field(default_factory=dict)
    # Reward-zone metrics (Rules 1-3)
    trade_count: int = 0           # Rule 1: price changes in history — 0 = dead market
    top4_liquidity_usd: float = 0.0  # USDC in top-4 bids+asks across all tokens
    reward_per_dollar: float = 0.0   # Rule 3: daily reward / zone liquidity (higher = better)
    orderbook_depth: int = 0       # total bid levels (display)
    min_order_cost: float = 0.0    # total market budget needed for min_size on selected outcome(s)
    farm_mode: str = "cheap"       # cheap | expensive | both | auto
    both_scan_mode: str = "cheap"  # cheap | strict, relevant only for farm_mode=both


def normalize_farm_mode(value: object) -> str:
    mode = str(value or "cheap").strip().lower()
    return mode if mode in FARM_MODES else "cheap"


def normalize_both_scan_mode(value: object) -> str:
    mode = str(value or "cheap").strip().lower()
    return mode if mode in BOTH_SCAN_MODES else "cheap"


def select_buy_token_index(tokens: list[dict], farm_mode: str = "cheap") -> int:
    """Select the configured outcome using the same rule in scan and placement."""
    if not tokens:
        raise ValueError("Cannot select a buy token from an empty list")
    valid_indexes = [
        index for index, token in enumerate(tokens)
        if 0 < float(token.get("price", 0) or 0) < 1
    ]
    indexes = valid_indexes or list(range(len(tokens)))
    chooser = max if normalize_farm_mode(farm_mode) == "expensive" else min
    return chooser(indexes, key=lambda index: float(tokens[index].get("price", 0) or 0))


def select_buy_token_indexes(tokens: list[dict], farm_mode: str = "cheap") -> tuple[int, ...]:
    """Return every outcome that must be quoted for the configured farm mode."""
    mode = normalize_farm_mode(farm_mode)
    # Auto intentionally scans like cheap mode. Both live books are inspected
    # only for the small set of markets that reaches the entry stage.
    if mode == "auto":
        return (select_buy_token_index(tokens, "cheap"),)
    if mode != "both":
        return (select_buy_token_index(tokens, mode),)
    if not tokens:
        raise ValueError("Cannot select buy tokens from an empty list")
    valid_indexes = tuple(
        index for index, token in enumerate(tokens)
        if 0 < float(token.get("price", 0) or 0) < 1
    )
    return valid_indexes or tuple(range(len(tokens)))


def select_filter_token_indexes(
    tokens: list[dict],
    farm_mode: str = "cheap",
    both_scan_mode: str = "cheap",
) -> tuple[int, ...]:
    """Return outcomes used by scanner/live book quality filters."""
    if (
        normalize_farm_mode(farm_mode) == "both"
        and normalize_both_scan_mode(both_scan_mode) == "cheap"
    ):
        return (select_buy_token_index(tokens, "cheap"),)
    return select_buy_token_indexes(tokens, farm_mode)


@dataclass(frozen=True)
class RewardTokenSnapshot:
    token_id: str
    outcome: str
    price: float


@dataclass(frozen=True)
class MultiRewardMarket:
    condition_id: str
    question: str
    market_slug: str
    event_slug: str
    rewards_min_size: float
    rewards_max_spread: float
    market_competitiveness: float
    tokens: tuple[RewardTokenSnapshot, ...]
    total_daily_rate: float
    end_date: str | None = None
    volume_24hr: float = 0.0


@dataclass
class _PreparedHybridMarket:
    reward: Any
    daily: float
    min_size: float
    max_spread: float
    competitiveness: float
    question: str
    market_slug: str
    event_slug: str
    token_list: list[dict]
    buy_indexes: tuple[int, ...]
    end_date_str: str | None
    farm_mode: str
    both_scan_mode: str
    filter_indexes: tuple[int, ...]


@dataclass
class _HybridBookCandidate:
    prepared: _PreparedHybridMarket
    buy_order_books: dict[int, Any]
    provisional_score: float


class HybridScanner:
    """Two-stage scanner for the raw multi rewards feed.

    A cheap in-memory pass is followed by one BUY-token book per candidate.
    Only the best/exploration subset receives the expensive remaining books,
    price history and market metadata requests.
    """

    def __init__(
        self,
        *,
        request_concurrency: int = 12,
        book_cache_ttl_s: float = 90.0,
        history_cache_ttl_s: float = 900.0,
        metadata_cache_ttl_s: float = 21600.0,
        cache_max_items: int = 4000,
    ) -> None:
        self._request_concurrency = max(1, int(request_concurrency))
        self._book_cache_ttl_s = max(0.0, float(book_cache_ttl_s))
        self._history_cache_ttl_s = max(0.0, float(history_cache_ttl_s))
        self._metadata_cache_ttl_s = max(0.0, float(metadata_cache_ttl_s))
        self._cache_max_items = max(100, int(cache_max_items))
        self._book_cache: dict[str, tuple[float, Any]] = {}
        self._history_cache: dict[str, tuple[float, Any]] = {}
        self._metadata_cache: dict[str, tuple[float, Any]] = {}
        self._last_deep_at: dict[str, float] = {}

    def prepare(
        self,
        candidates: list,
        *,
        min_spread: float,
        order_usdc: float,
        farm_mode: str = "cheap",
        both_scan_mode: str = "cheap",
        word_blacklist: list[str] | None = None,
    ) -> tuple[list[_PreparedHybridMarket], dict[str, Any]]:
        """Apply filters that need no network calls."""
        farm_mode = normalize_farm_mode(farm_mode)
        both_scan_mode = normalize_both_scan_mode(both_scan_mode)
        started = time.monotonic()
        prepared: list[_PreparedHybridMarket] = []
        rejected = {
            "bad_reward_data": 0,
            "narrow_reward_zone": 0,
            "no_tokens": 0,
            "extreme_price": 0,
            "ending_soon": 0,
            "question_blacklist": 0,
            "word_blacklist": 0,
            "over_budget": 0,
        }
        words = [str(w).strip().lower() for w in (word_blacklist or []) if str(w).strip()]
        min_end = datetime.now(timezone.utc) + timedelta(days=30)

        for reward in candidates:
            try:
                daily = float(getattr(reward, "total_daily_rate", 0) or 0)
                min_size = float(getattr(reward, "rewards_min_size", 0) or 20)
                spread_raw = getattr(reward, "rewards_max_spread", None)
                max_spread = float(spread_raw) if spread_raw is not None else 0.05
                if max_spread > 1:
                    max_spread /= 100
            except (TypeError, ValueError):
                rejected["bad_reward_data"] += 1
                continue

            if max_spread * 100 < float(min_spread):
                rejected["narrow_reward_zone"] += 1
                continue

            token_list: list[dict] = []
            for token in (getattr(reward, "tokens", None) or []):
                token_id = str(getattr(token, "token_id", "") or "")
                if not token_id:
                    continue
                try:
                    price = float(getattr(token, "price", 0) or 0)
                except (TypeError, ValueError):
                    price = 0.0
                token_list.append({
                    "token_id": token_id,
                    "outcome": str(getattr(token, "outcome", "") or ""),
                    "price": price,
                })
            if not token_list:
                rejected["no_tokens"] += 1
                continue

            yes_price = float(token_list[0]["price"])
            if not 0.15 <= yes_price <= 0.85:
                rejected["extreme_price"] += 1
                continue

            question = str(
                getattr(reward, "question", "")
                or getattr(reward, "condition_id", "")
            )
            question_lower = question.lower()
            if any(keyword in question_lower for keyword in QUESTION_BLACKLIST):
                rejected["question_blacklist"] += 1
                continue
            if any(word in question_lower for word in words):
                rejected["word_blacklist"] += 1
                continue

            end_date_str, end_dt = _normalise_end_date(getattr(reward, "end_date", None))
            if end_dt is not None and end_dt < min_end:
                rejected["ending_soon"] += 1
                continue

            buy_indexes = select_buy_token_indexes(token_list, farm_mode)
            if farm_mode == "both" and len(buy_indexes) != 2:
                rejected["no_tokens"] += 1
                continue
            filter_indexes = select_filter_token_indexes(
                token_list,
                farm_mode,
                both_scan_mode,
            )
            min_order_cost = (
                min_size * sum(
                    float(token_list[index]["price"])
                    for index in buy_indexes
                )
            )
            if order_usdc > 0 and min_order_cost > order_usdc + 1e-9:
                rejected["over_budget"] += 1
                continue

            prepared.append(_PreparedHybridMarket(
                reward=reward,
                daily=daily,
                min_size=min_size,
                max_spread=max_spread,
                competitiveness=_to_float_default(
                    getattr(reward, "market_competitiveness", 0),
                    0.0,
                ),
                question=question,
                market_slug=str(getattr(reward, "market_slug", "") or ""),
                event_slug=str(getattr(reward, "event_slug", "") or ""),
                token_list=token_list,
                buy_indexes=buy_indexes,
                end_date_str=end_date_str,
                farm_mode=farm_mode,
                both_scan_mode=both_scan_mode,
                filter_indexes=filter_indexes,
            ))

        return prepared, {
            "cheap_input": len(candidates),
            "cheap_passing": len(prepared),
            "cheap_rejected": len(candidates) - len(prepared),
            "cheap_rejected_by_reason": rejected,
            "cheap_duration_ms": round((time.monotonic() - started) * 1000, 1),
        }

    async def scan_prepared(
        self,
        candidates: list[_PreparedHybridMarket],
        *,
        category_blacklist: list[str] | None,
        volatility_threshold: float,
        max_ob_spread: float,
        max_daily_trades: int,
        max_bid_depth_spread: float,
        deep_limit: int = 30,
    ) -> tuple[list[ScoredMarket], dict[str, Any]]:
        started = time.monotonic()
        metrics: dict[str, Any] = {
            "book_candidates": len(candidates),
            "book_passing": 0,
            "deep_selected": 0,
            "deep_scored": 0,
            "book_requests": 0,
            "book_cache_hits": 0,
            "history_requests": 0,
            "history_cache_hits": 0,
            "metadata_requests": 0,
            "metadata_cache_hits": 0,
        }
        semaphore = asyncio.Semaphore(self._request_concurrency)

        async def fetch_cached(
            cache: dict[str, tuple[float, Any]],
            key: str,
            ttl_s: float,
            request_metric: str,
            hit_metric: str,
            fetch,
        ):
            now = time.monotonic()
            cached = cache.get(key)
            if cached is not None and now - cached[0] <= ttl_s:
                metrics[hit_metric] += 1
                return cached[1]
            metrics[request_metric] += 1
            async with semaphore:
                value = await fetch()
            if value is not None:
                cache[key] = (time.monotonic(), value)
                self._prune_cache(cache)
            return value

        async def load_buy_books(prepared: _PreparedHybridMarket):
            async def load(index: int):
                token_id = prepared.token_list[index]["token_id"]
                return await fetch_cached(
                    self._book_cache,
                    token_id,
                    self._book_cache_ttl_s,
                    "book_requests",
                    "book_cache_hits",
                    lambda: client.get_order_book(token_id),
                )

            results = await asyncio.gather(
                *(load(index) for index in prepared.filter_indexes),
                return_exceptions=True,
            )
            return {
                index: result
                for index, result in zip(prepared.filter_indexes, results)
                if not isinstance(result, BaseException) and result is not None
            }

        buy_books = await asyncio.gather(
            *(load_buy_books(candidate) for candidate in candidates),
            return_exceptions=True,
        )

        book_candidates: list[_HybridBookCandidate] = []
        for prepared, result in zip(candidates, buy_books):
            if (
                isinstance(result, BaseException)
                or len(result) != len(prepared.filter_indexes)
            ):
                continue
            selected_liquidity = 0.0
            books_valid = True
            for index in prepared.filter_indexes:
                buy_ob = result[index]
                spread = _ob_spread_cents(buy_ob)
                bids = _extract_bids(buy_ob)
                depth_spread = bid_depth_spread_cents(bids)
                if (
                    (spread is not None and spread > max_ob_spread)
                    or (
                        depth_spread is not None
                        and depth_spread > max_bid_depth_spread
                    )
                ):
                    books_valid = False
                    break
                buy_mid = mid_from_order_book(buy_ob) or float(
                    prepared.token_list[index]["price"]
                )
                liquidity = _zone_liquidity_usd(
                    buy_ob,
                    buy_mid,
                    prepared.max_spread,
                )
                if liquidity <= 0:
                    books_valid = False
                    break
                selected_liquidity += liquidity
            if not books_valid:
                continue
            book_candidates.append(_HybridBookCandidate(
                prepared=prepared,
                buy_order_books=result,
                provisional_score=prepared.daily / selected_liquidity,
            ))

        book_candidates.sort(key=lambda item: item.provisional_score, reverse=True)
        metrics["book_passing"] = len(book_candidates)
        selected = self._select_deep_candidates(book_candidates, deep_limit)
        metrics["deep_selected"] = len(selected)
        selected_at = time.monotonic()
        for item in selected:
            condition_id = str(getattr(item.prepared.reward, "condition_id", "") or "")
            if condition_id:
                self._last_deep_at[condition_id] = selected_at

        blacklist = {c.lower() for c in (category_blacklist or [])} | BLACKLISTED_CATEGORIES
        if "politics" in blacklist:
            blacklist |= POLITICS_TAG_VARIANTS

        async def deep_enrich(item: _HybridBookCandidate) -> ScoredMarket | None:
            prepared = item.prepared
            token_list = prepared.token_list
            yes_token_id = token_list[0]["token_id"]

            async def load_history():
                return await fetch_cached(
                    self._history_cache,
                    yes_token_id,
                    self._history_cache_ttl_s,
                    "history_requests",
                    "history_cache_hits",
                    lambda: client.get_price_history(yes_token_id, interval="1d"),
                )

            async def load_metadata():
                if not prepared.market_slug:
                    return None
                return await fetch_cached(
                    self._metadata_cache,
                    prepared.market_slug,
                    self._metadata_cache_ttl_s,
                    "metadata_requests",
                    "metadata_cache_hits",
                    lambda: client.get_market_by_slug(prepared.market_slug),
                )

            remaining_indexes = [
                index for index in range(len(token_list))
                if index not in prepared.filter_indexes
            ]

            async def load_other_book(index: int):
                token_id = token_list[index]["token_id"]
                return await fetch_cached(
                    self._book_cache,
                    token_id,
                    self._book_cache_ttl_s,
                    "book_requests",
                    "book_cache_hits",
                    lambda: client.get_order_book(token_id),
                )

            history_task = asyncio.create_task(load_history())
            metadata_task = asyncio.create_task(load_metadata())
            other_books = await asyncio.gather(
                *(load_other_book(index) for index in remaining_indexes),
                return_exceptions=True,
            )
            history, market = await asyncio.gather(history_task, metadata_task)

            all_obs: list[Any] = [None] * len(token_list)
            for index, order_book in item.buy_order_books.items():
                all_obs[index] = order_book
            for index, result in zip(remaining_indexes, other_books):
                if not isinstance(result, BaseException):
                    all_obs[index] = result

            all_bids_map: dict[str, list[float]] = {}
            top4_liq = 0.0
            total_depth = 0
            for token, order_book in zip(token_list, all_obs):
                bids = _extract_bids(order_book)
                all_bids_map[token["token_id"]] = bids
                total_depth += len(bids)
                token_mid = mid_from_order_book(order_book) or float(token["price"])
                top4_liq += _zone_liquidity_usd(
                    order_book,
                    token_mid,
                    prepared.max_spread,
                )
            if top4_liq <= 0:
                return None

            history = history or []
            volatility = _calc_volatility(history)
            if volatility > volatility_threshold:
                return None
            trade_count = _count_trades(history)
            if trade_count > max_daily_trades:
                return None

            category = ""
            end_date_str = prepared.end_date_str
            if market is not None:
                market_end = getattr(market, "end_date", None)
                state = getattr(market, "state", None)
                if market_end is None and state is not None:
                    market_end = getattr(state, "end_date", None)
                metadata_end_str, metadata_end_dt = _normalise_end_date(market_end)
                if end_date_str is None:
                    end_date_str = metadata_end_str
                if metadata_end_dt is not None:
                    min_end = datetime.now(timezone.utc) + timedelta(days=30)
                    if metadata_end_dt < min_end:
                        return None

                for tag in (getattr(market, "tags", None) or []):
                    label = str(getattr(tag, "label", "") or "").lower()
                    slug = str(getattr(tag, "slug", "") or "").lower()
                    if label in blacklist or slug in blacklist:
                        category = label or slug
                        break
                    if not category and (
                        label in PREFERRED_CATEGORIES or slug in PREFERRED_CATEGORIES
                    ):
                        category = label or slug
            if category in blacklist:
                return None

            yes_price = float(token_list[0]["price"])
            mid_price = yes_price if 0 < yes_price < 1 else 0.5
            reward_per_dollar = prepared.daily / top4_liq
            activity_factor = 1.0 / (trade_count / 10.0 + 1.0)
            score = reward_per_dollar * activity_factor
            min_order_cost = (
                prepared.min_size * sum(
                    float(token_list[index]["price"])
                    for index in prepared.buy_indexes
                )
            )

            return ScoredMarket(
                condition_id=str(getattr(prepared.reward, "condition_id", "") or ""),
                question=prepared.question,
                market_slug=prepared.market_slug,
                event_slug=prepared.event_slug,
                total_daily_rate=prepared.daily,
                rewards_min_size=prepared.min_size,
                rewards_max_spread=prepared.max_spread,
                market_competitiveness=prepared.competitiveness,
                price_volatility=round(volatility, 4),
                end_date=end_date_str,
                score=round(score, 6),
                tokens=token_list,
                category=category,
                mid_price=mid_price,
                orderbook_bids=all_bids_map,
                trade_count=trade_count,
                top4_liquidity_usd=round(top4_liq, 2),
                reward_per_dollar=round(reward_per_dollar, 6),
                orderbook_depth=total_depth,
                min_order_cost=round(min_order_cost, 2),
                farm_mode=prepared.farm_mode,
                both_scan_mode=prepared.both_scan_mode,
            )

        deep_results = await asyncio.gather(
            *(deep_enrich(item) for item in selected),
            return_exceptions=True,
        )
        scored = [
            result for result in deep_results
            if isinstance(result, ScoredMarket)
        ]
        for result in deep_results:
            if isinstance(result, BaseException):
                log.warning("Hybrid deep enrichment failed: %s", result)
        scored.sort(key=lambda market: market.score, reverse=True)
        metrics["deep_scored"] = len(scored)
        metrics["duration_ms"] = round((time.monotonic() - started) * 1000, 1)
        metrics["cache_sizes"] = {
            "books": len(self._book_cache),
            "history": len(self._history_cache),
            "metadata": len(self._metadata_cache),
        }
        log.info(
            "Hybrid batch: cheap=%d book_pass=%d deep=%d scored=%d requests(book=%d history=%d metadata=%d) duration=%.0fms",
            len(candidates),
            metrics["book_passing"],
            metrics["deep_selected"],
            metrics["deep_scored"],
            metrics["book_requests"],
            metrics["history_requests"],
            metrics["metadata_requests"],
            metrics["duration_ms"],
        )
        return scored, metrics

    def _select_deep_candidates(
        self,
        candidates: list[_HybridBookCandidate],
        limit: int,
    ) -> list[_HybridBookCandidate]:
        limit = max(0, min(int(limit), len(candidates)))
        if limit == 0:
            return []
        top_count = min(limit, max(1, int(limit * 2 / 3)))
        top = candidates[:top_count]
        top_ids = {
            str(getattr(item.prepared.reward, "condition_id", "") or "")
            for item in top
        }
        exploration = [
            item for item in candidates
            if str(getattr(item.prepared.reward, "condition_id", "") or "") not in top_ids
        ]
        exploration.sort(key=lambda item: (
            self._last_deep_at.get(
                str(getattr(item.prepared.reward, "condition_id", "") or ""),
                0.0,
            ),
            -item.provisional_score,
        ))
        return top + exploration[:limit - top_count]

    def _prune_cache(self, cache: dict[str, tuple[float, Any]]) -> None:
        overflow = len(cache) - self._cache_max_items
        if overflow <= 0:
            return
        oldest = sorted(cache.items(), key=lambda item: item[1][0])[:overflow]
        for key, _value in oldest:
            cache.pop(key, None)


def _normalise_end_date(value: Any) -> tuple[str | None, datetime | None]:
    if value is None or value == "":
        return None, None
    try:
        if isinstance(value, str):
            parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        else:
            parsed = value
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.isoformat(), parsed
    except (TypeError, ValueError, AttributeError):
        return None, None


async def scan_markets(
    min_daily_reward: float = 1.0,
    deposit_pct: float = 50.0,
    max_markets: int = 20,
    depth: str = "edge",
    category_blacklist: list[str] | None = None,
    volatility_threshold: float = 0.03,
    min_spread: float = 4.0,
    farm_mode: str = "cheap",
    both_scan_mode: str = "cheap",
) -> list[ScoredMarket]:
    """Legacy one-shot scan (used by /api/markets/refresh). Uses rotating logic internally."""
    all_rewards = await client.get_all_rewards()
    all_passing = [r for r in all_rewards if float(r.total_daily_rate or 0) >= min_daily_reward]
    all_passing.sort(key=lambda r: float(r.total_daily_rate or 0), reverse=True)
    high = [r for r in all_passing if float(r.total_daily_rate or 0) >= HIGH_RATE_CUTOFF][:HIGH_RATE_QUOTA]
    mid  = [r for r in all_passing if MID_RATE_CUTOFF <= float(r.total_daily_rate or 0) < HIGH_RATE_CUTOFF][:MID_RATE_QUOTA]
    gem_pool = [r for r in all_passing if float(r.total_daily_rate or 0) < MID_RATE_CUTOFF]
    _random.shuffle(gem_pool)
    batch = high + mid + gem_pool[:GEM_QUOTA]
    return await enrich_batch(
        batch, category_blacklist=category_blacklist,
        volatility_threshold=volatility_threshold, min_spread=min_spread,
        max_markets=max_markets, farm_mode=farm_mode,
        both_scan_mode=both_scan_mode,
    )


async def enrich_batch(
    candidates: list,
    category_blacklist: list[str] | None = None,
    volatility_threshold: float = 0.03,
    min_spread: float = 4.0,
    max_markets: int = 200,
    max_ob_spread: float = 2.0,
    max_daily_trades: int = 3,
    max_bid_depth_spread: float = 4.0,
    market_rewards: list | None = None,
    farm_mode: str = "cheap",
    both_scan_mode: str = "cheap",
) -> list[ScoredMarket]:
    """Enrich a pre-selected list of CurrentReward objects → ScoredMarket list."""
    blacklist = {c.lower() for c in (category_blacklist or [])} | BLACKLISTED_CATEGORIES
    # Expand politics blacklist with all known tag variants
    if "politics" in blacklist:
        blacklist |= POLITICS_TAG_VARIANTS

    scored: list[ScoredMarket] = []
    now = datetime.now(timezone.utc)
    min_end = now + timedelta(days=30)  # must have ≥30 days until expiry

    if market_rewards is None:
        market_rewards = await _batch_fetch_market_rewards(candidates)

    for reward, mr in zip(candidates, market_rewards):
        try:
            daily = float(reward.total_daily_rate or 0)
            if mr is None:
                continue

            min_size = float(mr.rewards_min_size or 20)
            _spread_raw = getattr(mr, "rewards_max_spread", None)
            max_spread = float(_spread_raw) if _spread_raw is not None else 0.05
            if max_spread > 1:
                max_spread = max_spread / 100

            # Filter: skip markets where spread is too narrow (< min_spread ¢)
            if max_spread * 100 < min_spread:
                log.debug("Skip %s: spread %.1f¢ < min %.1f¢", str(mr.question or "")[:40], max_spread * 100, min_spread)
                continue
            competitiveness = float(mr.market_competitiveness or 0.0)
            question = str(mr.question or reward.condition_id)
            market_slug = str(mr.market_slug or "")
            event_slug = str(mr.event_slug or "")

            token_list = []
            for t in (mr.tokens or []):
                token_id = str(t.token_id or "")
                outcome = str(t.outcome or "")
                price = float(t.price or 0)
                if token_id:
                    token_list.append({"token_id": token_id, "outcome": outcome, "price": price})

            if not token_list:
                continue

            # Filter: extreme markets (YES < 15¢ or > 85¢) — at 90/10 both sides needed.
            yes_price_raw = token_list[0]["price"]
            if not (0.15 <= yes_price_raw <= 0.85):
                log.debug(
                    "Skip %s: extreme price %.2f (need both sides for rewards)",
                    str(mr.question or "")[:40], yes_price_raw,
                )
                continue

            # ── Step 3: price history + order books for ALL tokens ────────────
            yes_token_id = token_list[0]["token_id"]
            ob_tasks = [client.get_order_book(t["token_id"]) for t in token_list]
            history, *all_obs = await asyncio.gather(
                client.get_price_history(yes_token_id, interval="1d"),
                *ob_tasks,
            )
            volatility = _calc_volatility(history)

            all_bids_map: dict[str, list[float]] = {}
            top4_liq = 0.0
            total_depth = 0
            ob_failures = 0
            for t, ob in zip(token_list, all_obs):
                if ob is None:
                    ob_failures += 1
                bids = _extract_bids(ob)
                all_bids_map[t["token_id"]] = bids
                total_depth += len(bids)
                token_mid = mid_from_order_book(ob) or float(t["price"])
                top4_liq += _zone_liquidity_usd(ob, token_mid, max_spread)

            # Hard filters on the token we will actually BUY.
            buy_indexes = select_buy_token_indexes(token_list, farm_mode)
            if normalize_farm_mode(farm_mode) == "both" and len(buy_indexes) != 2:
                continue
            filter_indexes = select_filter_token_indexes(
                token_list,
                farm_mode,
                both_scan_mode,
            )
            selected_books_valid = True
            for buy_idx in filter_indexes:
                buy_ob = (
                    all_obs[buy_idx]
                    if buy_idx < len(all_obs)
                    else None
                )
                buy_token_id = token_list[buy_idx]["token_id"]
                ob_spread = _ob_spread_cents(buy_ob)
                if ob_spread is not None and ob_spread > max_ob_spread:
                    log.debug(
                        "Skip %s: bid-ask spread %.1f¢ > max %.1f¢ (on selected token)",
                        question[:40], ob_spread, max_ob_spread,
                    )
                    selected_books_valid = False
                    break
                depth_spread = bid_depth_spread_cents(
                    all_bids_map.get(buy_token_id, [])
                )
                if depth_spread is not None and depth_spread > max_bid_depth_spread:
                    log.debug(
                        "Skip %s: bid depth spread %.1f¢ > max %.1f¢ (on selected token)",
                        question[:40], depth_spread, max_bid_depth_spread,
                    )
                    selected_books_valid = False
                    break
            if not selected_books_valid:
                continue

            # Hard filter: too volatile → skip regardless of competition
            if volatility > volatility_threshold:
                continue

            # ── Step 4: end date + category (Rule 2 check) ───────────────────
            end_date_str: str | None = None
            category = ""
            end_date_val = getattr(mr, "end_date", None)
            if end_date_val is not None:
                try:
                    if isinstance(end_date_val, str):
                        end_dt = datetime.fromisoformat(end_date_val.replace("Z", "+00:00"))
                    else:
                        end_dt = end_date_val
                        if end_dt.tzinfo is None:
                            end_dt = end_dt.replace(tzinfo=timezone.utc)
                    if end_dt < min_end:
                        continue
                    end_date_str = end_dt.isoformat()
                except (ValueError, AttributeError):
                    pass
            if market_slug:
                market = await client.get_market_by_slug(market_slug)
                if market:
                    end_date_val = getattr(market, "end_date", None)
                    state = getattr(market, "state", None)
                    if end_date_val is None and state:
                        end_date_val = getattr(state, "end_date", None)

                    if end_date_str is None and end_date_val is not None:
                        try:
                            if isinstance(end_date_val, str):
                                end_dt = datetime.fromisoformat(end_date_val.replace("Z", "+00:00"))
                            else:
                                end_dt = end_date_val
                                if end_dt.tzinfo is None:
                                    end_dt = end_dt.replace(tzinfo=timezone.utc)
                            if end_dt < min_end:
                                continue
                            end_date_str = end_dt.isoformat()
                        except (ValueError, AttributeError):
                            pass

                    for tag in (getattr(market, "tags", None) or []):
                        label = str(getattr(tag, "label", "") or "").lower()
                        slug_t = str(getattr(tag, "slug",  "") or "").lower()
                        if label in blacklist or slug_t in blacklist:
                            category = label or slug_t
                            break
                        if not category and (label in PREFERRED_CATEGORIES or slug_t in PREFERRED_CATEGORIES):
                            category = label or slug_t

            if category in blacklist:
                continue

            question_lower = question.lower()
            if any(kw in question_lower for kw in QUESTION_BLACKLIST):
                log.debug("Skipping blacklisted question: %s", question[:60])
                continue

            yes_price = token_list[0]["price"]
            mid_price = yes_price if 0 < yes_price < 1 else 0.5

            # Rule 1: dead market metric
            trade_count = _count_trades(history)

            # Hard filter: too many price changes per day → not a dead market, skip
            if trade_count > max_daily_trades:
                log.debug(
                    "Skip %s: %d price changes/day > max %d",
                    question[:40], trade_count, max_daily_trades,
                )
                continue

            # Rule 3: reward per dollar of zone liquidity
            # Skip if zone is empty — either dead market or all OBs failed (can't score reliably).
            if top4_liq == 0:
                log.debug("Skip %s: zero zone liquidity (ob_failures=%d/%d)", question[:40], ob_failures, len(token_list))
                continue
            reward_per_dollar = daily / top4_liq

            # Score = reward_per_dollar × dead-market bonus
            # dead-market bonus: 0 trade changes → ×1.0, 10 changes → ×0.5, 30 → ×0.25
            activity_factor = 1.0 / (trade_count / 10.0 + 1.0)
            score = reward_per_dollar * activity_factor

            log.debug(
                "%s | $/day=%.1f zone_liq=$%.2f rpd=%.4f trades=%d score=%.4f",
                question[:40], daily, top4_liq, reward_per_dollar, trade_count, score,
            )

            # Min order cost for the token the bot will buy.
            min_order_cost = round(
                min_size * sum(
                    float(token_list[index]["price"])
                    for index in buy_indexes
                ),
                2,
            )

            scored.append(ScoredMarket(
                condition_id=reward.condition_id,
                question=question,
                market_slug=market_slug,
                event_slug=event_slug,
                total_daily_rate=daily,
                rewards_min_size=min_size,
                rewards_max_spread=max_spread,
                market_competitiveness=competitiveness,
                price_volatility=round(volatility, 4),
                end_date=end_date_str,
                score=round(score, 6),
                tokens=token_list,
                category=category,
                mid_price=mid_price,
                orderbook_bids=all_bids_map,
                trade_count=trade_count,
                top4_liquidity_usd=round(top4_liq, 2),
                reward_per_dollar=round(reward_per_dollar, 6),
                orderbook_depth=total_depth,
                min_order_cost=min_order_cost,
                farm_mode=normalize_farm_mode(farm_mode),
                both_scan_mode=normalize_both_scan_mode(both_scan_mode),
            ))

        except Exception as e:
            log.warning("Error processing %s: %s", getattr(reward, "condition_id", "?"), e)

    scored.sort(key=lambda m: m.score, reverse=True)
    log.info("Batch enriched: %d scored from %d candidates", len(scored), len(candidates))
    return scored[:max_markets]


async def _batch_fetch_market_rewards(rewards, concurrency: int = 10):
    sem = asyncio.Semaphore(concurrency)

    async def fetch_one(r):
        async with sem:
            return await client.get_market_reward(r.condition_id)

    return await asyncio.gather(*[fetch_one(r) for r in rewards])


def multi_reward_from_api(item: dict[str, Any]) -> MultiRewardMarket | None:
    condition_id = str(item.get("condition_id") or "")
    if not condition_id:
        return None
    tokens = []
    for token in item.get("tokens") or []:
        if not isinstance(token, dict):
            continue
        token_id = str(token.get("token_id") or "")
        if not token_id:
            continue
        tokens.append(RewardTokenSnapshot(
            token_id=token_id,
            outcome=str(token.get("outcome") or ""),
            price=_to_float_default(token.get("price"), 0.0),
        ))
    if not tokens:
        return None
    daily = 0.0
    for cfg in item.get("rewards_config") or []:
        if isinstance(cfg, dict):
            daily += _to_float_default(cfg.get("rate_per_day"), 0.0)
    return MultiRewardMarket(
        condition_id=condition_id,
        question=str(item.get("question") or condition_id),
        market_slug=str(item.get("market_slug") or ""),
        event_slug=str(item.get("event_slug") or ""),
        rewards_min_size=_to_float_default(item.get("rewards_min_size"), 20.0),
        rewards_max_spread=_to_float_default(item.get("rewards_max_spread"), 0.05),
        market_competitiveness=_to_float_default(item.get("market_competitiveness"), 0.0),
        tokens=tuple(tokens),
        total_daily_rate=daily,
        end_date=str(item.get("end_date") or "") or None,
        volume_24hr=_to_float_default(item.get("volume_24hr"), 0.0),
    )


def _to_float_default(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def calc_order_price(
    mid_price: float,
    rewards_max_spread: float,
    depth: str,
    bids: list[float] | None = None,
) -> float:
    """BUY price inside the LP reward zone.

    Formula price (fallback):
      edge  → spread-1¢ from mid (bottom of reward zone)
      mid   → spread/2 from mid
      first → 1¢ from mid (top of zone, max reward share)

    Order-book improvement (edge and mid only): if the 2nd distinct bid level in the book is
    higher than the formula price AND still within the reward zone, we place
    there instead — higher in the queue, more rewards, but not the #1 bidder.
    """
    one_cent = 0.01
    if depth == "first":
        offset = one_cent
    elif depth == "mid":
        offset = rewards_max_spread / 2
    else:  # edge
        offset = max(rewards_max_spread - one_cent, one_cent)

    formula_price = max(0.001, min(0.999, round(mid_price - offset, 3)))

    # The "first" mode is handled by the bot against a live order book: it
    # joins the existing best bid. Do not apply the edge/mid second-level
    # shortcut here.
    if depth == "first":
        return formula_price

    if bids and len(bids) >= 2:
        # Deduplicate to get distinct price levels (0.1¢ precision), then pick 2nd highest
        levels = sorted(set(round(b, 3) for b in bids), reverse=True)
        if len(levels) >= 2:
            second_level = levels[1]
            lo = max(0.001, mid_price - rewards_max_spread)
            # Second level must be inside reward zone and strictly better than formula
            if lo <= second_level <= mid_price and second_level > formula_price:
                return second_level

    return formula_price


def _zone_liquidity_usd(order_book, mid_price: float, max_spread: float) -> float:
    """Total USDC in orders within the actual reward zone [mid±spread].
    Only counts orders that qualify for rewards — no orders outside the zone.
    """
    if order_book is None:
        return 0.0
    total = 0.0
    lo = max(0.01, mid_price - max_spread)
    hi = min(0.99, mid_price + max_spread)

    for b in (getattr(order_book, "bids", None) or []):
        p = getattr(b, "price", None)
        s = getattr(b, "size", None)
        if p is not None and s is not None:
            try:
                price, size = float(p), float(s)
                if lo <= price <= mid_price:
                    total += price * size
            except (TypeError, ValueError):
                pass

    for a in (getattr(order_book, "asks", None) or []):
        p = getattr(a, "price", None)
        s = getattr(a, "size", None)
        if p is not None and s is not None:
            try:
                price, size = float(p), float(s)
                if mid_price <= price <= hi:
                    total += price * size
            except (TypeError, ValueError):
                pass

    return total


def _calc_volatility(history: list) -> float:
    prices = []
    for pt in history:
        val = getattr(pt, "p", None) or getattr(pt, "price", None)
        if val is not None:
            prices.append(float(val))
    if len(prices) < 2:
        return 0.0
    try:
        return statistics.stdev(prices)
    except statistics.StatisticsError:
        return 0.0


def _count_trades(history: list) -> int:
    """Count price-change events in history = approximate trade activity.
    0 = completely dead (price never moved). Lower is better for farming.
    """
    prices = []
    for pt in history:
        val = getattr(pt, "p", None) or getattr(pt, "price", None)
        if val is not None:
            prices.append(float(val))
    if len(prices) < 2:
        return len(prices)
    return sum(1 for a, b in zip(prices, prices[1:]) if abs(a - b) > 0.001)


def _extract_bids(order_book) -> list[float]:
    """Sorted bid prices (highest first) for order placement, 0.1¢ precision."""
    if order_book is None:
        return []
    bids_raw = getattr(order_book, "bids", None) or []
    prices = []
    for b in bids_raw:
        p = getattr(b, "price", None)
        if p is not None:
            try:
                prices.append(round(float(p), 3))
            except (TypeError, ValueError):
                pass
    return sorted(prices, reverse=True)


def _extract_asks(order_book) -> list[float]:
    """Sorted ask prices (lowest first) for sell order placement, 0.1¢ precision."""
    if order_book is None:
        return []
    asks_raw = getattr(order_book, "asks", None) or []
    prices = []
    for a in asks_raw:
        p = getattr(a, "price", None)
        if p is not None:
            try:
                prices.append(round(float(p), 3))
            except (TypeError, ValueError):
                pass
    return sorted(prices)


def calc_sell_order_price(
    mid_price: float,
    rewards_max_spread: float,
    asks: list[float] | None = None,
) -> float:
    """SELL price inside the LP reward zone (ask side).

    Strategy: join the first (lowest) existing ask level inside the reward zone —
    stand in the first row without undercutting, so we earn LP rewards
    without becoming the cheapest seller and getting filled immediately.

    If no asks exist in the zone → open it at mid + 1¢ (minimum ask).
    """
    hi = min(0.999, mid_price + rewards_max_spread)
    fallback = max(0.001, min(0.999, round(mid_price + 0.01, 3)))

    if not asks:
        return fallback

    levels = sorted(set(round(a, 3) for a in asks))
    # Find the lowest ask that's inside the reward zone [mid, mid+spread]
    for level in levels:
        if mid_price <= level <= hi:
            return level  # join this row — don't undercut

    return fallback


def _ob_spread_cents(order_book) -> float | None:
    """Actual bid-ask spread in cents from order book. None if insufficient data."""
    if order_book is None:
        return None
    bids_raw = getattr(order_book, "bids", None) or []
    asks_raw = getattr(order_book, "asks", None) or []
    bids = sorted([float(b.price) for b in bids_raw if getattr(b, "price", None)], reverse=True)
    asks = sorted([float(a.price) for a in asks_raw if getattr(a, "price", None)])
    if not bids or not asks:
        return None
    return (asks[0] - bids[0]) * 100


def bid_depth_spread_cents(bids: list[float]) -> float | None:
    """Spread between 1st and 4th unique bid levels in cents.
    Less than 4 levels means the book is too thin and should fail the filter.
    """
    levels = sorted(set(round(b, 2) for b in bids), reverse=True)
    if len(levels) < 4:
        return 999.0
    return round((levels[0] - levels[3]) * 100, 1)


def mid_from_order_book(order_book) -> float | None:
    """Estimate current mid price from best bid/ask."""
    if order_book is None:
        return None
    bids_raw = getattr(order_book, "bids", None) or []
    asks_raw = getattr(order_book, "asks", None) or []
    bids = sorted([float(b.price) for b in bids_raw if getattr(b, "price", None)], reverse=True)
    asks = sorted([float(a.price) for a in asks_raw if getattr(a, "price", None)])
    best_bid = bids[0] if bids else None
    best_ask = asks[0] if asks else None
    if best_bid and best_ask:
        return (best_bid + best_ask) / 2
    return best_bid or best_ask
