import os
import time
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch


os.environ["KEYSTORE_FILE"] = ""
os.environ["PRIVATE_KEY"] = "0x" + "11" * 32
os.environ.setdefault("WALLET_ADDRESS", "0x" + "22" * 20)
os.environ.setdefault("API_KEY", "test")

from src.bot import (  # noqa: E402
    FarmingBot,
    _ComplementShadowWatch,
    _complement_shadow_metrics,
)
from src.market_ws import MarketSignalSnapshot, MarketWsWatcher  # noqa: E402


def book(bids, asks):
    return SimpleNamespace(
        bids=[SimpleNamespace(price=str(price), size=str(size)) for price, size in bids],
        asks=[SimpleNamespace(price=str(price), size=str(size)) for price, size in asks],
    )


def signal(
    asset_id,
    *,
    bid,
    ask,
    trade_price=None,
    trade_side="",
    trade_size=0,
    trade_at=0,
):
    return MarketSignalSnapshot(
        asset_id=asset_id,
        best_bid=bid,
        best_ask=ask,
        last_trade_price=trade_price,
        last_trade_side=trade_side,
        last_trade_size=trade_size,
        last_trade_at=trade_at,
        updated_at=time.monotonic(),
    )


class ComplementShadowTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.watch = _ComplementShadowWatch(
            order_id="order-1",
            condition_id="condition-1",
            token_id="yes",
            complement_token_id="no",
            outcome="YES",
            order_price=0.42,
            market_question="Test market",
        )

    def test_complement_move_and_trade_produce_shadow_cancel_signal(self):
        now = time.monotonic()
        direct = book([(0.44, 100), (0.43, 100), (0.42, 100)], [(0.46, 100)])
        complement = book([(0.56, 100), (0.55, 100)], [(0.58, 100), (0.59, 100)])

        metrics = _complement_shadow_metrics(
            self.watch,
            direct,
            complement,
            signal("yes", bid=0.44, ask=0.46),
            signal(
                "no",
                bid=0.56,
                ask=0.58,
                trade_price=0.56,
                trade_side="BUY",
                trade_size=25,
                trade_at=now,
            ),
            {
                "at": now - 1,
                "complement_bid": 0.55,
                "queue_ahead": 87,
            },
            now,
        )

        self.assertGreaterEqual(metrics["score"], 60)
        self.assertTrue(metrics["would_cancel"])
        self.assertIn("complement_bid_up", metrics["reasons"])
        self.assertIn("complement_buy_trade", metrics["reasons"])

    def test_pair_sum_near_one_is_a_hard_shadow_signal(self):
        now = time.monotonic()
        direct = book([(0.41, 100), (0.40, 100)], [(0.43, 100)])
        complement = book([(0.57, 100), (0.56, 100)], [(0.59, 100)])

        metrics = _complement_shadow_metrics(
            self.watch,
            direct,
            complement,
            signal("yes", bid=0.41, ask=0.43),
            signal("no", bid=0.57, ask=0.59),
            None,
            now,
        )

        self.assertEqual(metrics["score"], 0)
        self.assertTrue(metrics["hard_risk"])
        self.assertTrue(metrics["would_cancel"])
        self.assertIn("pair_sum_near_one", metrics["reasons"])

    async def test_pair_sum_hard_risk_is_logged_immediately_with_zero_score(self):
        farming_bot = FarmingBot()
        direct = book([(0.41, 100), (0.40, 100)], [(0.43, 100)])
        complement = book([(0.57, 100), (0.56, 100)], [(0.59, 100)])
        snapshots = {
            "yes": signal("yes", bid=0.41, ask=0.43),
            "no": signal("no", bid=0.57, ask=0.59),
        }
        books = {"yes": direct, "no": complement}
        farming_bot._market_ws.get_order_book = AsyncMock(
            side_effect=lambda asset_id: books[asset_id]
        )
        farming_bot._market_ws.get_signal_snapshot = AsyncMock(
            side_effect=lambda asset_id: snapshots[asset_id]
        )

        with patch("src.bot.shadow_log.info", Mock()) as log_info:
            await farming_bot._evaluate_complement_shadow(
                self.watch,
                triggered_asset="no",
            )

        log_info.assert_called_once()
        args = log_info.call_args.args
        self.assertEqual(args[1], "signal")
        self.assertTrue(args[2])
        self.assertEqual(args[3], 0)
        self.assertEqual(farming_bot._shadow_would_cancel_count, 1)

    async def test_market_watcher_subscribes_to_both_registered_outcomes(self):
        watcher = MarketWsWatcher()
        watcher.register_market_tokens("condition-1", ["yes", "no"])

        with patch(
            "src.market_ws.db.get_active_positions",
            AsyncMock(
                return_value=[
                    {
                        "condition_id": "condition-1",
                        "token_id": "yes",
                    }
                ]
            ),
        ):
            assets = await watcher._active_assets()

        self.assertEqual(assets, {"yes", "no"})

    async def test_fast_position_lookup_uses_replaced_ram_snapshot(self):
        farming_bot = FarmingBot()
        farming_bot._replace_active_positions_cache(
            [
                {
                    "order_id": "order-1",
                    "condition_id": "condition-1",
                    "token_id": "yes",
                    "side": "BUY",
                    "status": "OPEN",
                }
            ]
        )

        with patch(
            "src.bot.db.get_active_positions",
            AsyncMock(side_effect=AssertionError("unexpected SQLite read")),
        ):
            position = await farming_bot._cached_open_buy("yes")

        self.assertEqual(position["order_id"], "order-1")

    async def test_runtime_config_load_reads_settings_in_one_batch(self):
        farming_bot = FarmingBot()
        with patch(
            "src.bot.db.get_all_settings",
            AsyncMock(return_value={"depth": "first", "order_usdc": 19}),
        ) as get_all:
            cfg = await farming_bot._load_cfg()

        get_all.assert_awaited_once()
        self.assertEqual(cfg["depth"], "first")
        self.assertEqual(cfg["order_usdc"], 19)
        self.assertIs(await farming_bot._cached_runtime_cfg(), cfg)

    def test_market_token_maps_are_pruned_to_candidates_and_active_positions(self):
        farming_bot = FarmingBot()
        farming_bot._market_tokens_by_condition = {
            f"old-{idx}": (f"yes-{idx}", f"no-{idx}")
            for idx in range(120)
        }
        farming_bot._market_tokens_by_condition["candidate"] = (
            "candidate-yes",
            "candidate-no",
        )
        farming_bot._market_tokens_by_condition["active"] = (
            "active-yes",
            "active-no",
        )
        farming_bot.last_scan = [SimpleNamespace(condition_id="candidate")]

        farming_bot._replace_active_positions_cache(
            [
                {
                    "order_id": "active-order",
                    "condition_id": "active",
                    "token_id": "active-yes",
                    "side": "BUY",
                    "status": "OPEN",
                }
            ]
        )

        self.assertEqual(
            set(farming_bot._market_tokens_by_condition),
            {"candidate", "active"},
        )
        self.assertEqual(
            set(farming_bot._market_ws._market_tokens),
            {"candidate", "active"},
        )


if __name__ == "__main__":
    unittest.main()
