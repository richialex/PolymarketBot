from __future__ import annotations

import os
import unittest
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ["KEYSTORE_FILE"] = ""
os.environ["PRIVATE_KEY"] = "0x" + "11" * 32
os.environ.setdefault("WALLET_ADDRESS", "0x" + "22" * 20)
os.environ.setdefault("API_KEY", "test")

from src.bot import FarmingBot  # noqa: E402
from src.scanner import ScoredMarket  # noqa: E402


def order_book(mid: float):
    return SimpleNamespace(
        bids=[
            SimpleNamespace(price=str(mid - 0.01), size="100"),
            SimpleNamespace(price=str(mid - 0.02), size="100"),
            SimpleNamespace(price=str(mid - 0.03), size="100"),
            SimpleNamespace(price=str(mid - 0.04), size="100"),
        ],
        asks=[
            SimpleNamespace(price=str(mid + 0.01), size="100"),
            SimpleNamespace(price=str(mid + 0.02), size="100"),
        ],
    )


def market() -> ScoredMarket:
    return ScoredMarket(
        condition_id="condition-1",
        question="Test two-sided market",
        market_slug="test-market",
        event_slug="test-event",
        total_daily_rate=20,
        rewards_min_size=10,
        rewards_max_spread=0.05,
        market_competitiveness=0.1,
        price_volatility=0,
        end_date=None,
        score=1,
        tokens=[
            {"token_id": "yes", "outcome": "YES", "price": 0.4},
            {"token_id": "no", "outcome": "NO", "price": 0.6},
        ],
        mid_price=0.4,
        orderbook_bids={"yes": [0.39, 0.38], "no": [0.59, 0.58]},
        min_order_cost=6,
        farm_mode="both",
    )


def cfg() -> dict:
    return {
        "farm_mode": "both",
        "depth": "edge",
        "max_ob_spread": 3,
        "max_bid_depth_spread": 4,
        "target_level_share_enabled": False,
    }


class TwoSidedFarmTests(unittest.IsolatedAsyncioTestCase):
    async def test_existing_single_sided_modes_still_place_one_expected_outcome(self) -> None:
        for farm_mode, expected_token in (("cheap", "yes"), ("expensive", "no")):
            with self.subTest(farm_mode=farm_mode):
                bot = FarmingBot()
                placed_positions: list[dict] = []

                async def get_book(token_id: str):
                    return order_book(0.4 if token_id == "yes" else 0.6)

                async def place(position: dict):
                    stored = {
                        **position,
                        "order_id": f"remote-{position['token_id']}",
                        "status": "OPEN",
                    }
                    placed_positions.append(stored)
                    return SimpleNamespace(ok=True, ambiguous=False), stored

                single_cfg = {**cfg(), "farm_mode": farm_mode}
                with (
                    patch("src.bot.db.is_market_banned", AsyncMock(return_value=False)),
                    patch("src.bot.order_manager.can_place", AsyncMock(return_value=True)),
                    patch("src.bot.client.get_order_book", AsyncMock(side_effect=get_book)),
                    patch("src.bot.order_manager.place_position", AsyncMock(side_effect=place)),
                ):
                    spent = await bot._enter_market(market(), 10, single_cfg)

                self.assertGreater(spent, 0)
                self.assertEqual(
                    [position["token_id"] for position in placed_positions],
                    [expected_token],
                )

    async def test_places_equal_share_pair_and_returns_total_quote_cost(self) -> None:
        bot = FarmingBot()
        placed_positions: list[dict] = []

        async def get_book(token_id: str):
            return order_book(0.4 if token_id == "yes" else 0.6)

        async def place(position: dict):
            stored = {**position, "order_id": f"remote-{position['token_id']}", "status": "OPEN"}
            placed_positions.append(stored)
            return SimpleNamespace(ok=True, ambiguous=False), stored

        with (
            patch("src.bot.db.is_market_banned", AsyncMock(return_value=False)),
            patch("src.bot.order_manager.can_place", AsyncMock(return_value=True)),
            patch("src.bot.client.get_order_book", AsyncMock(side_effect=get_book)),
            patch("src.bot.order_manager.place_position", AsyncMock(side_effect=place)),
        ):
            spent = await bot._enter_market(market(), 10, cfg())

        self.assertEqual(len(placed_positions), 2)
        self.assertEqual(len({position["size"] for position in placed_positions}), 1)
        self.assertGreaterEqual(placed_positions[0]["size"], 10)
        self.assertEqual({position["farm_mode"] for position in placed_positions}, {"both"})
        self.assertEqual(len({position["entry_group_id"] for position in placed_positions}), 1)
        self.assertAlmostEqual(
            spent,
            sum(position["price"] * position["size"] for position in placed_positions),
        )
        self.assertLessEqual(spent, 10)

    async def test_mid_depth_is_applied_independently_to_both_sides(self) -> None:
        bot = FarmingBot()
        placed_positions: list[dict] = []

        async def get_book(token_id: str):
            return order_book(0.4 if token_id == "yes" else 0.6)

        async def place(position: dict):
            stored = {**position, "order_id": f"remote-{position['token_id']}", "status": "OPEN"}
            placed_positions.append(stored)
            return SimpleNamespace(ok=True, ambiguous=False), stored

        mid_cfg = {**cfg(), "depth": "mid"}
        with (
            patch("src.bot.db.is_market_banned", AsyncMock(return_value=False)),
            patch("src.bot.order_manager.can_place", AsyncMock(return_value=True)),
            patch("src.bot.client.get_order_book", AsyncMock(side_effect=get_book)),
            patch("src.bot.order_manager.place_position", AsyncMock(side_effect=place)),
        ):
            await bot._enter_market(market(), 10, mid_cfg)

        prices = {position["token_id"]: position["price"] for position in placed_positions}
        self.assertEqual(prices, {"no": 0.58, "yes": 0.38})

    async def test_soft_search_filters_quality_on_cheap_side_only(self) -> None:
        async def get_book(token_id: str):
            if token_id == "yes":
                return order_book(0.4)
            return SimpleNamespace(
                bids=[
                    SimpleNamespace(price="0.59", size="100"),
                    SimpleNamespace(price="0.58", size="100"),
                    SimpleNamespace(price="0.57", size="100"),
                    SimpleNamespace(price="0.56", size="100"),
                ],
                asks=[SimpleNamespace(price="0.66", size="100")],
            )

        async def run(scan_mode: str) -> int:
            bot = FarmingBot()
            placed: list[dict] = []

            async def place(position: dict):
                stored = {**position, "order_id": f"remote-{position['token_id']}", "status": "OPEN"}
                placed.append(stored)
                return SimpleNamespace(ok=True, ambiguous=False), stored

            mode_cfg = {**cfg(), "both_scan_mode": scan_mode}
            with (
                patch("src.bot.db.is_market_banned", AsyncMock(return_value=False)),
                patch("src.bot.order_manager.can_place", AsyncMock(return_value=True)),
                patch("src.bot.client.get_order_book", AsyncMock(side_effect=get_book)),
                patch("src.bot.order_manager.place_position", AsyncMock(side_effect=place)),
            ):
                await bot._enter_market(market(), 10, mode_cfg)
            return len(placed)

        self.assertEqual(await run("cheap"), 2)
        self.assertEqual(await run("strict"), 0)

    async def test_second_side_failure_rolls_back_first_side(self) -> None:
        bot = FarmingBot()
        calls = 0

        async def get_book(token_id: str):
            return order_book(0.4 if token_id == "yes" else 0.6)

        async def place(position: dict):
            nonlocal calls
            calls += 1
            if calls == 1:
                placed = {**position, "order_id": "remote-first", "status": "OPEN"}
                return SimpleNamespace(ok=True, ambiguous=False), placed
            return (
                SimpleNamespace(
                    ok=False,
                    ambiguous=False,
                    error_code="rejected",
                    error_message="test rejection",
                ),
                {**position, "status": "FAILED"},
            )

        with (
            patch("src.bot.db.is_market_banned", AsyncMock(return_value=False)),
            patch("src.bot.order_manager.can_place", AsyncMock(return_value=True)),
            patch("src.bot.client.get_order_book", AsyncMock(side_effect=get_book)),
            patch("src.bot.order_manager.place_position", AsyncMock(side_effect=place)),
            patch("src.bot.order_manager.cancel_order", AsyncMock(return_value=True)) as cancel,
        ):
            spent = await bot._enter_market(market(), 10, cfg())

        self.assertEqual(spent, 0)
        cancel.assert_awaited_once_with(
            "remote-first",
            reason="paired_entry_rollback",
        )

    async def test_rebalance_cancels_both_orders_in_persisted_pair(self) -> None:
        bot = FarmingBot()
        worst = replace(market(), condition_id="worst", score=1)
        best = replace(market(), condition_id="best", score=2)
        pair = [
            {
                "order_id": "yes-order",
                "condition_id": "worst",
                "side": "BUY",
                "entry_group_id": "pair-1",
            },
            {
                "order_id": "no-order",
                "condition_id": "worst",
                "side": "BUY",
                "entry_group_id": "pair-1",
            },
        ]

        with (
            patch("src.bot.db.get_open_positions", AsyncMock(return_value=pair)),
            patch("src.bot.db.get_active_positions", AsyncMock(return_value=pair)),
            patch("src.bot.order_manager.cancel_order", AsyncMock(return_value=True)) as cancel,
        ):
            await bot._rebalance_risk(
                [best, worst],
                slots_remaining=0,
                slot_value=10,
                max_slots_per_market=1,
            )

        self.assertEqual(cancel.await_count, 2)
        self.assertEqual(
            {call.args[0] for call in cancel.await_args_list},
            {"yes-order", "no-order"},
        )


if __name__ == "__main__":
    unittest.main()
