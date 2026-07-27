from __future__ import annotations

import os
import unittest
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ["KEYSTORE_FILE"] = ""
os.environ["PRIVATE_KEY"] = "0x" + "11" * 32
os.environ.setdefault("WALLET_ADDRESS", "0x" + "22" * 20)
os.environ.setdefault("API_KEY", "test")

from src.scanner import (  # noqa: E402
    HybridScanner,
    MultiRewardMarket,
    RewardTokenSnapshot,
    select_buy_token_index,
    select_buy_token_indexes,
)


def reward_market(
    index: int,
    *,
    question: str | None = None,
    min_size: float = 20,
    max_spread: float = 0.05,
    yes_price: float = 0.4,
) -> MultiRewardMarket:
    return MultiRewardMarket(
        condition_id=f"condition-{index}",
        question=question or f"Quiet market {index}",
        market_slug=f"market-{index}",
        event_slug=f"event-{index}",
        rewards_min_size=min_size,
        rewards_max_spread=max_spread,
        market_competitiveness=0.1,
        tokens=(
            RewardTokenSnapshot(f"yes-{index}", "Yes", yes_price),
            RewardTokenSnapshot(f"no-{index}", "No", 1 - yes_price),
        ),
        total_daily_rate=10 + index,
        end_date=(datetime.now(timezone.utc) + timedelta(days=90)).isoformat(),
    )


def order_book(mid: float):
    return SimpleNamespace(
        bids=[
            SimpleNamespace(price=str(mid), size="100"),
            SimpleNamespace(price=str(mid - 0.01), size="100"),
            SimpleNamespace(price=str(mid - 0.02), size="100"),
            SimpleNamespace(price=str(mid - 0.03), size="100"),
        ],
        asks=[
            SimpleNamespace(price=str(mid + 0.01), size="100"),
            SimpleNamespace(price=str(mid + 0.02), size="100"),
        ],
    )


class HybridScannerTests(unittest.IsolatedAsyncioTestCase):
    def test_buy_token_selection_defaults_to_cheap_and_supports_expensive(self) -> None:
        tokens = [
            {"token_id": "yes", "outcome": "Yes", "price": 0.3},
            {"token_id": "no", "outcome": "No", "price": 0.7},
        ]

        self.assertEqual(select_buy_token_index(tokens), 0)
        self.assertEqual(select_buy_token_index(tokens, "expensive"), 1)
        self.assertEqual(select_buy_token_index(tokens, "unknown"), 0)
        self.assertEqual(select_buy_token_indexes(tokens, "both"), (0, 1))

    def test_prepare_applies_budget_to_configured_side(self) -> None:
        scanner = HybridScanner()
        markets = [reward_market(1, min_size=20, yes_price=0.4)]

        cheap, _ = scanner.prepare(
            markets,
            min_spread=2,
            order_usdc=10,
            farm_mode="cheap",
        )
        expensive, metrics = scanner.prepare(
            markets,
            min_spread=2,
            order_usdc=10,
            farm_mode="expensive",
        )

        self.assertEqual(cheap[0].buy_indexes, (0,))
        self.assertEqual(expensive, [])
        self.assertEqual(metrics["cheap_rejected_by_reason"]["over_budget"], 1)

    def test_prepare_two_sided_budget_must_cover_each_minimum(self) -> None:
        scanner = HybridScanner()
        markets = [reward_market(1, min_size=20, yes_price=0.4)]

        rejected, metrics = scanner.prepare(
            markets,
            min_spread=2,
            order_usdc=10,
            farm_mode="both",
        )
        accepted, _ = scanner.prepare(
            markets,
            min_spread=2,
            order_usdc=20,
            farm_mode="both",
        )

        self.assertEqual(rejected, [])
        self.assertEqual(metrics["cheap_rejected_by_reason"]["over_budget"], 1)
        self.assertEqual(accepted[0].buy_indexes, (0, 1))
        self.assertEqual(accepted[0].filter_indexes, (0,))

        strict, _ = scanner.prepare(
            markets,
            min_spread=2,
            order_usdc=20,
            farm_mode="both",
            both_scan_mode="strict",
        )
        self.assertEqual(strict[0].filter_indexes, (0, 1))

    def test_prepare_rejects_candidates_without_network_calls(self) -> None:
        scanner = HybridScanner()
        markets = [
            reward_market(1),
            reward_market(2, question="Election result"),
            reward_market(3, min_size=100, yes_price=0.4),
            reward_market(4, max_spread=0.01),
            reward_market(5, yes_price=0.05),
            reward_market(6, question="Forbidden topic"),
        ]

        prepared, metrics = scanner.prepare(
            markets,
            min_spread=2,
            order_usdc=20,
            word_blacklist=["forbidden"],
        )

        self.assertEqual([p.reward.condition_id for p in prepared], ["condition-1"])
        self.assertEqual(metrics["cheap_input"], 6)
        self.assertEqual(metrics["cheap_passing"], 1)
        self.assertEqual(metrics["cheap_rejected_by_reason"]["question_blacklist"], 1)
        self.assertEqual(metrics["cheap_rejected_by_reason"]["word_blacklist"], 1)
        self.assertEqual(metrics["cheap_rejected_by_reason"]["over_budget"], 1)
        self.assertEqual(metrics["cheap_rejected_by_reason"]["narrow_reward_zone"], 1)
        self.assertEqual(metrics["cheap_rejected_by_reason"]["extreme_price"], 1)

    async def test_deep_requests_are_capped_after_buy_book_filter(self) -> None:
        scanner = HybridScanner(request_concurrency=3)
        prepared, _metrics = scanner.prepare(
            [reward_market(i) for i in range(10)],
            min_spread=2,
            order_usdc=20,
        )

        async def get_book(token_id: str):
            return order_book(0.4 if token_id.startswith("yes") else 0.6)

        history = [
            SimpleNamespace(p=0.4),
            SimpleNamespace(p=0.4),
        ]
        metadata = SimpleNamespace(tags=[], end_date=None, state=None)
        with (
            patch("src.scanner.client.get_order_book", AsyncMock(side_effect=get_book)) as books,
            patch("src.scanner.client.get_price_history", AsyncMock(return_value=history)) as histories,
            patch("src.scanner.client.get_market_by_slug", AsyncMock(return_value=metadata)) as metadata_calls,
        ):
            scored, metrics = await scanner.scan_prepared(
                prepared,
                category_blacklist=[],
                volatility_threshold=0.05,
                max_ob_spread=2,
                max_daily_trades=3,
                max_bid_depth_spread=4,
                deep_limit=3,
            )

        self.assertEqual(metrics["book_candidates"], 10)
        self.assertEqual(metrics["book_passing"], 10)
        self.assertEqual(metrics["deep_selected"], 3)
        self.assertEqual(metrics["deep_scored"], 3)
        self.assertEqual(histories.await_count, 3)
        self.assertEqual(metadata_calls.await_count, 3)
        self.assertEqual(books.await_count, 13)  # 10 BUY books + 3 other-token books
        self.assertEqual(len(scored), 3)

    async def test_repeated_scan_uses_all_three_caches(self) -> None:
        scanner = HybridScanner()
        prepared, _metrics = scanner.prepare(
            [reward_market(1)],
            min_spread=2,
            order_usdc=20,
        )
        history = [SimpleNamespace(p=0.4), SimpleNamespace(p=0.4)]
        metadata = SimpleNamespace(tags=[], end_date=None, state=None)
        with (
            patch(
                "src.scanner.client.get_order_book",
                AsyncMock(side_effect=[
                    order_book(0.4),
                    order_book(0.6),
                ]),
            ) as books,
            patch("src.scanner.client.get_price_history", AsyncMock(return_value=history)) as histories,
            patch("src.scanner.client.get_market_by_slug", AsyncMock(return_value=metadata)) as metadata_calls,
        ):
            first, first_metrics = await scanner.scan_prepared(
                prepared,
                category_blacklist=[],
                volatility_threshold=0.05,
                max_ob_spread=2,
                max_daily_trades=3,
                max_bid_depth_spread=4,
                deep_limit=1,
            )
            second, second_metrics = await scanner.scan_prepared(
                prepared,
                category_blacklist=[],
                volatility_threshold=0.05,
                max_ob_spread=2,
                max_daily_trades=3,
                max_bid_depth_spread=4,
                deep_limit=1,
            )

        self.assertEqual(len(first), 1)
        self.assertEqual(len(second), 1)
        self.assertEqual(first_metrics["book_requests"], 2)
        self.assertEqual(second_metrics["book_requests"], 0)
        self.assertEqual(second_metrics["book_cache_hits"], 2)
        self.assertEqual(second_metrics["history_cache_hits"], 1)
        self.assertEqual(second_metrics["metadata_cache_hits"], 1)
        self.assertEqual(books.await_count, 2)
        self.assertEqual(histories.await_count, 1)
        self.assertEqual(metadata_calls.await_count, 1)


if __name__ == "__main__":
    unittest.main()
