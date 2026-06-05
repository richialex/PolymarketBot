"""Market scanning, filtering, and scoring logic."""
from __future__ import annotations

import asyncio
import logging
import random as _random
import statistics
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta

from src.pm_client import client

log = logging.getLogger(__name__)

BLACKLISTED_CATEGORIES: set[str] = set()
QUESTION_BLACKLIST: set[str] = {
    "democratic party", "republican party", "trump", "biden", "harris",
    "election", "popular vote", "congress", "senate", "house of representatives",
    "russia", "russian",
}
PREFERRED_CATEGORIES = {"gaming", "crypto", "sports", "esports", "technology", "token"}

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
    min_order_cost: float = 0.0    # cheapest token × min_size (min USDC to qualify for rewards)


async def scan_markets(
    min_daily_reward: float = 1.0,
    deposit_pct: float = 50.0,
    max_markets: int = 20,
    depth: str = "edge",
    category_blacklist: list[str] | None = None,
    volatility_threshold: float = 0.03,
    min_spread: float = 4.0,
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
        max_markets=max_markets,
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
) -> list[ScoredMarket]:
    """Enrich a pre-selected list of CurrentReward objects → ScoredMarket list."""
    blacklist = {c.lower() for c in (category_blacklist or [])} | BLACKLISTED_CATEGORIES
    # Expand politics blacklist with all known tag variants
    if "politics" in blacklist:
        blacklist |= POLITICS_TAG_VARIANTS

    scored: list[ScoredMarket] = []
    now = datetime.now(timezone.utc)
    min_end = now + timedelta(days=30)  # must have ≥30 days until expiry

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

            # Hard filter: bid-ask spread too wide on the token we will actually BUY.
            # We buy the expensive token (price > 0.50), so check its spread — not always YES.
            expensive_idx = max(range(len(token_list)), key=lambda i: float(token_list[i]["price"]))
            expensive_ob = all_obs[expensive_idx] if expensive_idx < len(all_obs) else all_obs[0] if all_obs else None
            expensive_token_id = token_list[expensive_idx]["token_id"]
            ob_spread = _ob_spread_cents(expensive_ob)
            if ob_spread is not None and ob_spread > max_ob_spread:
                log.debug(
                    "Skip %s: bid-ask spread %.1f¢ > max %.1f¢ (on expensive token)",
                    question[:40], ob_spread, max_ob_spread,
                )
                continue

            # Hard filter: bid depth too thin on the token we will actually BUY.
            depth_spread = bid_depth_spread_cents(all_bids_map.get(expensive_token_id, []))
            if depth_spread is not None and depth_spread > max_bid_depth_spread:
                log.debug(
                    "Skip %s: bid depth spread %.1f¢ > max %.1f¢ (on expensive token)",
                    question[:40], depth_spread, max_bid_depth_spread,
                )
                continue

            # Hard filter: too volatile → skip regardless of competition
            if volatility > volatility_threshold:
                continue

            # ── Step 4: end date + category (Rule 2 check) ───────────────────
            end_date_str: str | None = None
            category = ""
            if market_slug:
                market = await client.get_market_by_slug(market_slug)
                if market:
                    end_date_val = getattr(market, "end_date", None)
                    state = getattr(market, "state", None)
                    if end_date_val is None and state:
                        end_date_val = getattr(state, "end_date", None)

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

            # Min order cost = expensive token × min_size (bot always buys the expensive side)
            expensive_price = max(
                (float(t["price"]) for t in token_list if 0 < float(t["price"]) < 1),
                default=mid_price,
            )
            min_order_cost = round(min_size * expensive_price, 2)

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

    Order-book improvement: if the 2nd distinct bid level in the book is
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
