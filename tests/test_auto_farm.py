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

from src.bot import (  # noqa: E402
    FarmingBot,
    _BuyPlan,
    _optimize_auto_allocation,
)
from src.scanner import ScoredMarket, select_buy_token_indexes  # noqa: E402


def plan(token_id: str, price: float, weight: float, budget: float = 20) -> _BuyPlan:
    return _BuyPlan(
        token={"token_id": token_id, "outcome": token_id.upper(), "price": price},
        target_price=price,
        max_budget=budget,
        reward_weight=weight,
    )


def old_auto_position(
    condition_id: str,
    order_id: str,
    *,
    price: float = 0.5,
    size: float = 20,
) -> dict:
    return {
        "condition_id": condition_id,
        "order_id": order_id,
        "market_question": f"Market {condition_id}",
        "token_id": f"token-{order_id}",
        "outcome": "YES",
        "side": "BUY",
        "price": price,
        "size": size,
        "status": "OPEN",
        "farm_mode": "auto",
        "entry_group_id": f"auto-{condition_id}",
        "placed_at": (
            datetime.now(timezone.utc) - timedelta(minutes=10)
        ).isoformat(),
    }


def order_book(mid: float):
    return SimpleNamespace(
        bids=[
            SimpleNamespace(price=str(mid - 0.01), size="100"),
            SimpleNamespace(price=str(mid - 0.02), size="100"),
            SimpleNamespace(price=str(mid - 0.03), size="100"),
            SimpleNamespace(price=str(mid - 0.04), size="100"),
        ],
        asks=[SimpleNamespace(price=str(mid + 0.01), size="100")],
    )


class AutoFarmTests(unittest.IsolatedAsyncioTestCase):
    def test_scanner_auto_mode_keeps_cheap_side_candidate_count(self) -> None:
        tokens = [
            {"token_id": "yes", "price": 0.35},
            {"token_id": "no", "price": 0.65},
        ]
        self.assertEqual(
            select_buy_token_indexes(tokens, "auto"),
            select_buy_token_indexes(tokens, "cheap"),
        )

    def test_optimizer_selects_weighted_two_sided_qmin_when_it_scores_best(self) -> None:
        allocation = _optimize_auto_allocation(
            [plan("cheap", 0.36, 0.04), plan("expensive", 0.56, 0.04)],
            total_budget=15,
            min_size=10,
        )
        self.assertIsNotNone(allocation)
        assert allocation is not None
        self.assertEqual(allocation.route, "both")
        self.assertGreaterEqual(min(allocation.sizes), 10)
        self.assertLessEqual(allocation.cost, 15)

    def test_optimizer_can_prefer_one_side_instead_of_forcing_a_pair(self) -> None:
        allocation = _optimize_auto_allocation(
            [plan("cheap", 0.4, 1.0), plan("expensive", 0.6, 0.01)],
            total_budget=10,
            min_size=10,
        )
        self.assertIsNotNone(allocation)
        assert allocation is not None
        self.assertEqual(allocation.route, "cheap")
        self.assertGreater(allocation.sizes[0], 0)
        self.assertEqual(allocation.sizes[1], 0)

    async def test_auto_entry_persists_selected_route_as_one_group(self) -> None:
        bot = FarmingBot()
        market = ScoredMarket(
            condition_id="condition-auto",
            question="Test auto market",
            market_slug="test-auto",
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
            farm_mode="auto",
        )
        placed: list[dict] = []

        async def place_position(position: dict):
            stored = {
                **position,
                "order_id": f"remote-{position['token_id']}",
                "status": "OPEN",
            }
            placed.append(stored)
            return SimpleNamespace(ok=True, ambiguous=False), stored

        cfg = {
            "farm_mode": "auto",
            "both_scan_mode": "cheap",
            "depth": "edge",
            "max_ob_spread": 3,
            "max_bid_depth_spread": 4,
            "target_level_share_enabled": False,
        }
        with (
            patch("src.bot.db.is_market_banned", AsyncMock(return_value=False)),
            patch("src.bot.order_manager.can_place", AsyncMock(return_value=True)),
            patch(
                "src.bot.client.get_order_book",
                AsyncMock(side_effect=lambda token_id: order_book(
                    0.4 if token_id == "yes" else 0.6
                )),
            ),
            patch(
                "src.bot.order_manager.place_position",
                AsyncMock(side_effect=place_position),
            ),
        ):
            spent = await bot._enter_market(market, 15, cfg)

        self.assertGreater(spent, 0)
        self.assertEqual(len(placed), 2)
        self.assertEqual({position["farm_mode"] for position in placed}, {"auto"})
        self.assertEqual(len({position["entry_group_id"] for position in placed}), 1)
        self.assertTrue(placed[0]["entry_group_id"].startswith("auto-"))

    async def test_low_share_is_cancelled_and_persistently_cooled_down(self) -> None:
        bot = FarmingBot()
        positions = [old_auto_position("cid-1", "order-1")]
        cfg = {
            "auto_reward_check_interval_s": 60,
            "auto_min_reward_share_pct": 0.5,
            "auto_target_reward_share_pct": 1.0,
            "auto_low_share_confirmations": 1,
            "auto_reject_cooldown_s": 7200,
            "order_usdc": 20,
            "auto_step_usdc": 5,
        }
        with (
            patch("src.bot.db.get_open_positions", AsyncMock(return_value=positions)),
            patch(
                "src.bot.client.get_reward_percentages",
                AsyncMock(return_value=({"cid-1": 0.1}, True)),
            ) as percentages,
            patch(
                "src.bot.order_manager.cancel_order",
                AsyncMock(return_value=True),
            ) as cancel,
            patch(
                "src.bot.db.set_condition_retry_delay",
                AsyncMock(),
            ) as cooldown,
        ):
            await bot._check_auto_reward_shares(cfg)

        percentages.assert_awaited_once()
        cancel.assert_awaited_once_with(
            "order-1",
            reason="auto_low_reward_share",
        )
        cooldown.assert_awaited_once_with("cid-1", 7200)

    async def test_non_auto_modes_add_no_reward_network_calls(self) -> None:
        bot = FarmingBot()
        positions = [{
            **old_auto_position("cid-1", "order-1"),
            "farm_mode": "cheap",
        }]
        get_positions = AsyncMock(return_value=positions)
        percentages = AsyncMock()
        with (
            patch("src.bot.db.get_open_positions", get_positions),
            patch("src.bot.client.get_reward_percentages", percentages),
        ):
            await bot._check_auto_reward_shares({
                "auto_reward_check_interval_s": 180,
            })
            await bot._check_auto_reward_shares({
                "auto_reward_check_interval_s": 180,
            })

        get_positions.assert_awaited_once()
        percentages.assert_not_awaited()

    async def test_middle_share_tops_up_without_extra_percentage_requests(self) -> None:
        bot = FarmingBot()
        positions = [
            old_auto_position("cid-1", "order-1"),
            old_auto_position("cid-2", "order-2"),
        ]
        cfg = {
            "auto_reward_check_interval_s": 60,
            "auto_min_reward_share_pct": 0.5,
            "auto_target_reward_share_pct": 1.0,
            "auto_low_share_confirmations": 2,
            "auto_reject_cooldown_s": 7200,
            "order_usdc": 15,
            "auto_step_usdc": 5,
            "free_balance_buffer_pct": 10,
            "bot_capital_limit_usdc": 100,
            "target_level_share_enabled": False,
        }
        with (
            patch("src.bot.db.get_open_positions", AsyncMock(return_value=positions)),
            patch(
                "src.bot.client.get_reward_percentages",
                AsyncMock(return_value=({"cid-1": 0.75, "cid-2": 1.2}, True)),
            ) as percentages,
            patch("src.bot.client.get_balance", AsyncMock(return_value=100)),
            patch("src.bot.db.get_bot_quote_notional", AsyncMock(return_value=20)),
            patch.object(bot, "_replace_open_order", AsyncMock(return_value=True)) as replace,
        ):
            await bot._check_auto_reward_shares(cfg)

        percentages.assert_awaited_once()
        replace.assert_awaited_once()
        self.assertEqual(replace.await_args.args[2], 30)


if __name__ == "__main__":
    unittest.main()
