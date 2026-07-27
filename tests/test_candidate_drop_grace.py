from __future__ import annotations

import os
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch


os.environ["KEYSTORE_FILE"] = ""
os.environ["PRIVATE_KEY"] = "0x" + "11" * 32
os.environ.setdefault("WALLET_ADDRESS", "0x" + "22" * 20)
os.environ.setdefault("API_KEY", "test")

from src.bot import FarmingBot  # noqa: E402


def open_position() -> dict:
    return {
        "order_id": "order-1",
        "condition_id": "condition-1",
        "market_question": "Test candidate grace market",
        "side": "BUY",
    }


def candidate():
    return SimpleNamespace(
        condition_id="condition-1",
        end_date=None,
        price_volatility=0.0,
        trade_count=0,
    )


class CandidateDropGraceTests(unittest.IsolatedAsyncioTestCase):
    async def test_absence_counts_once_per_completed_scan(self) -> None:
        bot = FarmingBot()
        cancel = AsyncMock(return_value=True)
        cfg = {
            "candidate_drop_confirm_scans": 3,
            "min_daily_reward": 0,
            "volatility_threshold": 1,
            "max_daily_trades": 100,
        }
        with (
            patch(
                "src.bot.db.get_open_positions",
                AsyncMock(return_value=[open_position()]),
            ),
            patch("src.bot.order_manager.cancel_order", cancel),
        ):
            bot._scan_generation = 1
            await bot._exit_stale_positions([], cfg)
            await bot._exit_stale_positions([], cfg)
            cancel.assert_not_awaited()

            bot._scan_generation = 2
            await bot._exit_stale_positions([], cfg)
            cancel.assert_not_awaited()

            bot._scan_generation = 3
            await bot._exit_stale_positions([], cfg)

        cancel.assert_awaited_once_with(
            "order-1",
            reason="dropped from candidates for 3 consecutive scans",
        )

    async def test_reappearance_resets_missing_scan_count(self) -> None:
        bot = FarmingBot()
        cancel = AsyncMock(return_value=True)
        cfg = {
            "candidate_drop_confirm_scans": 2,
            "min_daily_reward": 0,
            "volatility_threshold": 1,
            "max_daily_trades": 100,
        }
        with (
            patch(
                "src.bot.db.get_open_positions",
                AsyncMock(return_value=[open_position()]),
            ),
            patch("src.bot.order_manager.cancel_order", cancel),
        ):
            bot._scan_generation = 1
            await bot._exit_stale_positions([], cfg)
            bot._scan_generation = 2
            await bot._exit_stale_positions([candidate()], cfg)
            bot._scan_generation = 3
            await bot._exit_stale_positions([], cfg)
            cancel.assert_not_awaited()
            bot._scan_generation = 4
            await bot._exit_stale_positions([], cfg)

        cancel.assert_awaited_once()

    async def test_hard_reward_drop_still_cancels_immediately(self) -> None:
        bot = FarmingBot()
        bot._scan_generation = 1
        bot._rewards_cache = [
            SimpleNamespace(condition_id="condition-1", total_daily_rate=1.0)
        ]
        cancel = AsyncMock(return_value=True)
        with (
            patch(
                "src.bot.db.get_open_positions",
                AsyncMock(return_value=[open_position()]),
            ),
            patch("src.bot.order_manager.cancel_order", cancel),
        ):
            await bot._exit_stale_positions([], {
                "candidate_drop_confirm_scans": 3,
                "min_daily_reward": 7,
                "volatility_threshold": 1,
                "max_daily_trades": 100,
            })

        cancel.assert_awaited_once()
        self.assertIn("reward dropped", cancel.await_args.kwargs["reason"])


if __name__ == "__main__":
    unittest.main()
