from __future__ import annotations

import os
import tempfile
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch


# Avoid loading an operator keystore while importing the API wrapper.
os.environ["KEYSTORE_FILE"] = ""
os.environ["PRIVATE_KEY"] = "0x" + "11" * 32
os.environ.setdefault("WALLET_ADDRESS", "0x" + "22" * 20)
os.environ.setdefault("API_KEY", "test")

from src import db  # noqa: E402
from src.order_manager import OrderManager  # noqa: E402
from src.pm_client import PMClient, PlacementResult, _classify_order_error  # noqa: E402
from src.bot import FarmingBot  # noqa: E402
from polymarket.errors import RequestRejectedError, TransportError  # noqa: E402


def buy(order_id: str = "buy-1", size: float = 100.0) -> dict:
    return {
        "order_id": order_id,
        "condition_id": "condition-1",
        "market_question": "Test market",
        "token_id": "token-1",
        "outcome": "YES",
        "side": "BUY",
        "price": 0.2,
        "size": size,
        "status": "OPEN",
        "placed_at": "2026-07-26T00:00:00+00:00",
        "filled_at": None,
        "matched_size": 0,
        "reward_earned": 0,
        "parent_order_id": None,
        "local_id": None,
        "source": "BOT",
    }


class OrderLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.old_path = db.DB_PATH
        db.DB_PATH = Path(self.tmp.name) / "test.db"
        await db.init_db()

    async def asyncTearDown(self) -> None:
        db.DB_PATH = self.old_path
        self.tmp.cleanup()

    async def test_cumulative_updates_create_only_positive_fill_deltas(self) -> None:
        await db.upsert_position(buy())

        first = await db.record_cumulative_match("buy-1", 20, source="TEST")
        duplicate = await db.record_cumulative_match("buy-1", 20, source="TEST")
        second = await db.record_cumulative_match("buy-1", 35, source="TEST")

        self.assertEqual(first["fill_delta"], 20)
        self.assertEqual(duplicate["fill_delta"], 0)
        self.assertEqual(second["fill_delta"], 15)
        self.assertEqual(second["matched_size"], 35)
        self.assertEqual(second["status"], "PARTIALLY_FILLED")

    async def test_matched_status_is_full_only_when_quantity_is_full(self) -> None:
        await db.upsert_position(buy(size=50))
        partial = await db.record_cumulative_match("buy-1", 12)
        full = await db.record_cumulative_match("buy-1", 50)

        self.assertEqual(partial["status"], "PARTIALLY_FILLED")
        self.assertEqual(full["status"], "FILLED")

    async def test_exit_required_transition_preserves_execution_time(self) -> None:
        await db.upsert_position(buy(size=46))
        event_at = "2026-07-26T23:40:30.474545+00:00"

        filled = await db.record_cumulative_match(
            "buy-1",
            46,
            source="CANCEL_RECONCILE",
            event_at=event_at,
        )
        await db.update_position_status("buy-1", "EXIT_REQUIRED")
        stored = await db.get_position("buy-1")

        self.assertEqual(filled["filled_at"], event_at)
        self.assertEqual(stored["status"], "EXIT_REQUIRED")
        self.assertEqual(stored["filled_at"], event_at)

    async def test_init_db_repairs_missing_execution_time(self) -> None:
        position = buy(size=46)
        position.update(
            {
                "matched_size": 46,
                "status": "EXIT_REQUIRED",
                "filled_at": None,
            }
        )
        await db.upsert_position(position)

        await db.init_db()
        stored = await db.get_position("buy-1")

        self.assertEqual(stored["filled_at"], position["placed_at"])

    async def test_exit_accounting_uses_sold_and_working_remainders(self) -> None:
        position = buy(size=30)
        position.update({"matched_size": 30, "status": "EXIT_REQUIRED"})
        await db.upsert_position(position)

        sold = {
            **position,
            "order_id": "sell-1",
            "side": "SELL",
            "size": 30,
            "matched_size": 10,
            "status": "CANCELLED",
            "parent_order_id": "buy-1",
        }
        await db.upsert_position(sold)
        accounting = await db.get_parent_exit_accounting("buy-1")
        self.assertEqual(accounting["acquired"], 30)
        self.assertEqual(accounting["sold"], 10)
        self.assertEqual(accounting["uncovered"], 20)

        working = {
            **sold,
            "order_id": "sell-2",
            "size": 20,
            "matched_size": 0,
            "status": "OPEN",
        }
        await db.upsert_position(working)
        accounting = await db.get_parent_exit_accounting("buy-1")
        self.assertEqual(accounting["working_sell"], 20)
        self.assertEqual(accounting["uncovered"], 0)

    async def test_quote_notional_is_separate_from_matched_inventory(self) -> None:
        first = buy("buy-1", size=50)
        second = {
            **buy("buy-2", size=50),
            "condition_id": "condition-2",
            "token_id": "token-2",
        }
        await db.upsert_position(first)
        await db.upsert_position(second)
        self.assertEqual(await db.get_bot_quote_notional(), 20.0)
        self.assertEqual(await db.get_bot_inventory_exposure(), 0.0)

        await db.record_cumulative_match("buy-1", 10)
        self.assertEqual(await db.get_bot_quote_notional(), 18.0)
        self.assertEqual(await db.get_bot_inventory_exposure(), 2.0)

    async def test_partial_buy_cancels_remainder_and_uses_final_cancel_quantity(self) -> None:
        await db.upsert_position(buy(size=100))
        manager = OrderManager()
        callback = AsyncMock()
        manager.set_execution_callback(callback)

        remote = SimpleNamespace(size_matched=25, status="CANCELED")
        with (
            patch("src.order_manager.client.cancel_order", AsyncMock(return_value=True)),
            patch("src.order_manager.client.get_order", AsyncMock(return_value=remote)),
        ):
            updated = await manager.handle_order_update(
                order_id="buy-1",
                matched_size=20,
                status="LIVE",
                order_type="UPDATE",
            )
            await __import__("asyncio").sleep(0)

        stored = await db.get_position("buy-1")
        self.assertEqual(stored["matched_size"], 25)
        self.assertEqual(stored["status"], "EXIT_REQUIRED")
        self.assertEqual(updated["matched_size"], 25)
        callback.assert_awaited()

    async def test_partial_sell_is_not_marked_filled_and_retries_only_remainder(self) -> None:
        parent = buy(size=30)
        parent.update({"matched_size": 30, "status": "EXITING"})
        await db.upsert_position(parent)
        sell = {
            **parent,
            "order_id": "sell-1",
            "side": "SELL",
            "size": 30,
            "matched_size": 0,
            "status": "OPEN",
            "parent_order_id": "buy-1",
        }
        await db.upsert_position(sell)

        manager = OrderManager()
        callback = AsyncMock()
        manager.set_execution_callback(callback)
        remote = SimpleNamespace(size_matched=12, status="CANCELED")
        with (
            patch("src.order_manager.client.cancel_order", AsyncMock(return_value=True)),
            patch("src.order_manager.client.get_order", AsyncMock(return_value=remote)),
        ):
            await manager.handle_order_update(
                order_id="sell-1",
                matched_size=12,
                status="LIVE",
                order_type="UPDATE",
            )
            await __import__("asyncio").sleep(0)

        stored = await db.get_position("sell-1")
        accounting = await db.get_parent_exit_accounting("buy-1")
        self.assertEqual(stored["status"], "CANCELLED")
        self.assertEqual(stored["matched_size"], 12)
        self.assertEqual(accounting["sold"], 12)
        self.assertEqual(accounting["uncovered"], 18)
        callback.assert_awaited()

    async def test_partial_buy_rescue_places_sell_for_exact_acquired_size(self) -> None:
        position = buy(size=73)
        position.update({"matched_size": 48.92, "status": "EXIT_REQUIRED"})
        await db.upsert_position(position)
        bot = FarmingBot()
        book = SimpleNamespace(
            bids=[SimpleNamespace(price="0.19", size="100")],
            asks=[SimpleNamespace(price="0.20", size="100")],
            min_order_size="5",
        )
        placed_sell = {
            **position,
            "order_id": "sell-1",
            "side": "SELL",
            "size": 48.92,
            "status": "RECONCILE_REQUIRED",
            "parent_order_id": "buy-1",
        }
        response = SimpleNamespace(ok=True, order_id="sell-1", status="MATCHED")
        cfg = {
            "sell_mode": "market_after_delay",
            "market_sell_delay_s": 30,
            "market_sell_policy": "max_gap",
            "market_sell_max_gap_cents": 4,
        }

        with (
            patch.object(bot, "_get_order_book_ws_first", AsyncMock(return_value=book)),
            patch("src.bot.client.get_token_position_size_status", AsyncMock(return_value=(48.92, True))),
            patch("src.bot.order_manager.place_position", AsyncMock(return_value=(response, placed_sell))) as place,
        ):
            ok = await bot._place_exit_sell_for_buy(
                position,
                cfg,
                {},
                "partial_fill_rescue",
                force_immediate=True,
            )

        self.assertTrue(ok)
        payload = place.await_args.args[0]
        self.assertAlmostEqual(payload["size"], 48.92)
        self.assertEqual(payload["side"], "SELL")
        self.assertFalse(place.await_args.kwargs["post_only"])
        self.assertFalse(place.await_args.kwargs["market_sell"])

    async def test_missing_filled_at_does_not_restart_sell_delay_forever(self) -> None:
        position = buy(size=46)
        position.update(
            {
                "price": 0.30,
                "matched_size": 46,
                "status": "EXIT_REQUIRED",
                "placed_at": (
                    datetime.now(timezone.utc) - timedelta(hours=8)
                ).isoformat(),
                "filled_at": None,
            }
        )
        await db.upsert_position(position)
        bot = FarmingBot()
        book = SimpleNamespace(
            bids=[SimpleNamespace(price="0.20", size="100")],
            asks=[SimpleNamespace(price="0.21", size="100")],
            min_order_size="5",
        )
        placed_sell = {
            **position,
            "order_id": "sell-maker",
            "side": "SELL",
            "status": "OPEN",
            "parent_order_id": "buy-1",
        }
        response = SimpleNamespace(ok=True, order_id="sell-maker", status="LIVE")
        cfg = {
            "sell_mode": "market_after_delay",
            "market_sell_delay_s": 30,
            "market_sell_policy": "max_gap",
            "market_sell_max_gap_cents": 4,
        }

        with (
            patch.object(bot, "_get_order_book_ws_first", AsyncMock(return_value=book)),
            patch(
                "src.bot.client.get_token_position_size_status",
                AsyncMock(return_value=(46.0, True)),
            ),
            patch(
                "src.bot.order_manager.place_position",
                AsyncMock(return_value=(response, placed_sell)),
            ) as place,
        ):
            ok = await bot._place_exit_sell_for_buy(
                position,
                cfg,
                {},
                "recover_sell",
            )

        self.assertTrue(ok)
        self.assertTrue(place.await_args.kwargs["post_only"])
        self.assertFalse(place.await_args.kwargs["market_sell"])

    async def test_external_manual_sale_retires_stale_exit_required(self) -> None:
        position = buy(size=46)
        position.update(
            {
                "matched_size": 46,
                "status": "EXIT_REQUIRED",
                "filled_at": (
                    datetime.now(timezone.utc) - timedelta(minutes=10)
                ).isoformat(),
            }
        )
        await db.upsert_position(position)
        bot = FarmingBot()

        with (
            patch(
                "src.bot.client.get_token_position_size_status",
                AsyncMock(return_value=(0.0, True)),
            ),
            patch.object(
                bot,
                "_get_order_book_ws_first",
                AsyncMock(
                    side_effect=AssertionError(
                        "no order book is needed after verified external exit"
                    )
                ),
            ),
        ):
            ok = await bot._place_exit_sell_for_buy(
                position,
                {"sell_mode": "market_after_delay"},
                {},
                "recover_sell",
            )

        stored = await db.get_position("buy-1")
        self.assertTrue(ok)
        self.assertEqual(stored["status"], "EXITED")

    async def test_partial_dust_uses_market_fak_with_best_bid_floor(self) -> None:
        position = buy(size=73)
        position.update({"matched_size": 2.5, "status": "EXIT_REQUIRED"})
        await db.upsert_position(position)
        bot = FarmingBot()
        book = SimpleNamespace(
            bids=[SimpleNamespace(price="0.19", size="100")],
            asks=[SimpleNamespace(price="0.20", size="100")],
            min_order_size="5",
        )
        placed_sell = {
            **position,
            "order_id": "sell-dust",
            "side": "SELL",
            "size": 2.5,
            "status": "RECONCILE_REQUIRED",
            "parent_order_id": "buy-1",
        }
        response = SimpleNamespace(ok=True, order_id="sell-dust", status="MATCHED")

        with (
            patch.object(bot, "_get_order_book_ws_first", AsyncMock(return_value=book)),
            patch("src.bot.client.get_token_position_size_status", AsyncMock(return_value=(2.5, True))),
            patch("src.bot.order_manager.place_position", AsyncMock(return_value=(response, placed_sell))) as place,
        ):
            ok = await bot._place_exit_sell_for_buy(
                position,
                {"sell_mode": "maker"},
                {},
                "partial_fill_rescue",
                force_immediate=True,
            )

        self.assertTrue(ok)
        payload = place.await_args.args[0]
        self.assertAlmostEqual(payload["size"], 2.5)
        self.assertAlmostEqual(payload["price"], 0.19)
        self.assertTrue(place.await_args.kwargs["market_sell"])
        self.assertAlmostEqual(place.await_args.kwargs["min_price"], 0.19)
        self.assertFalse(place.await_args.kwargs["post_only"])

    async def test_order_manager_routes_market_sell_without_limit_order(self) -> None:
        manager = OrderManager()
        position = {
            **buy("sell-dust", size=2.5),
            "side": "SELL",
            "status": "SELL_PENDING",
            "parent_order_id": "buy-1",
        }
        accepted = PlacementResult(
            outcome="accepted",
            order_id="sell-dust-remote",
            status="live",
        )
        market_mock = AsyncMock(return_value=accepted)
        limit_mock = AsyncMock()
        with (
            patch("src.order_manager.client.place_market_sell", market_mock),
            patch("src.order_manager.client.place_limit", limit_mock),
        ):
            result, placed = await manager.place_position(
                position,
                post_only=False,
                pending_status="SELL_PENDING",
                market_sell=True,
                min_price=0.19,
            )

        self.assertTrue(result.ok)
        self.assertEqual(placed["order_id"], "sell-dust-remote")
        market_mock.assert_awaited_once_with("token-1", 2.5, min_price=0.19)
        limit_mock.assert_not_awaited()

    async def test_market_sell_uses_fak_shares_and_price_floor(self) -> None:
        sdk = MagicMock()
        sdk.place_market_order.return_value = SimpleNamespace(
            ok=True,
            order_id="sell-dust-remote",
            status="matched",
        )
        pm_client = PMClient()
        with patch.object(pm_client, "_secure", return_value=sdk):
            result = await pm_client.place_market_sell(
                "token-1",
                2.5,
                min_price=0.19,
            )

        self.assertTrue(result.ok)
        kwargs = sdk.place_market_order.call_args.kwargs
        self.assertEqual(kwargs["side"], "SELL")
        self.assertEqual(str(kwargs["shares"]), "2.5")
        self.assertEqual(str(kwargs["min_price"]), "0.19")
        self.assertEqual(kwargs["order_type"], "FAK")

    async def test_parent_is_exited_only_after_all_acquired_inventory_is_sold(self) -> None:
        position = buy(size=30)
        position.update({"matched_size": 30, "status": "EXITING"})
        await db.upsert_position(position)
        await db.upsert_position({
            **position,
            "order_id": "sell-1",
            "side": "SELL",
            "size": 30,
            "matched_size": 30,
            "status": "FILLED",
            "parent_order_id": "buy-1",
        })

        bot = FarmingBot()
        completed = await bot._place_exit_sell_for_buy(
            position,
            {},
            {},
            "sell_complete",
        )

        self.assertTrue(completed)
        stored = await db.get_position("buy-1")
        self.assertEqual(stored["status"], "EXITED")

    async def test_definitive_rejection_is_aggregated_without_failed_position_spam(self) -> None:
        manager = OrderManager()
        rejected = PlacementResult(
            outcome="rejected",
            error_code="not_enough_balance",
            error_message="not enough balance / allowance",
        )
        place_mock = AsyncMock(return_value=rejected)
        with patch("src.order_manager.client.place_limit", place_mock):
            result, placed = await manager.place_position(buy())
            suppressed, suppressed_pos = await manager.place_position({
                **buy("buy-2"),
                "placed_at": "2026-07-26T00:01:00+00:00",
            })

        self.assertFalse(result.ok)
        self.assertEqual(placed["status"], "FAILED")
        self.assertIsNone(await db.get_position("buy-1"))
        self.assertEqual(suppressed.error_code, "local_cooldown")
        self.assertEqual(suppressed_pos["status"], "SUPPRESSED")
        self.assertEqual(place_mock.await_count, 1)

        attempts = await db.get_order_attempts()
        self.assertEqual(len(attempts), 1)
        self.assertEqual(attempts[0]["attempt_count"], 1)
        self.assertEqual(attempts[0]["rejected_count"], 1)
        self.assertEqual(attempts[0]["last_error_code"], "not_enough_balance")
        self.assertGreater(await db.get_condition_retry_delay("condition-1"), 250)

    async def test_ambiguous_post_remains_reconcilable(self) -> None:
        manager = OrderManager()
        ambiguous = PlacementResult(
            outcome="ambiguous",
            error_code="transport_ambiguous",
            error_message="server disconnected",
        )
        with patch(
            "src.order_manager.client.place_limit",
            AsyncMock(return_value=ambiguous),
        ):
            result, placed = await manager.place_position(buy())

        self.assertTrue(result.ambiguous)
        self.assertEqual(placed["status"], "RECONCILE_REQUIRED")
        self.assertEqual((await db.get_position("buy-1"))["status"], "RECONCILE_REQUIRED")
        attempts = await db.get_order_attempts()
        self.assertEqual(attempts[0]["ambiguous_count"], 1)

    async def test_failure_backoff_survives_manager_restart(self) -> None:
        position = buy()
        await db.begin_order_attempt(position)
        first_manager = OrderManager()
        first_delay = await first_manager._record_failure(
            "condition-1",
            "token-1",
            "not_enough_balance",
            "BUY",
        )
        await db.finish_order_attempt(
            position,
            outcome="REJECTED",
            error_code="not_enough_balance",
            retry_delay_s=first_delay,
        )

        restarted_manager = OrderManager()
        second_delay = await restarted_manager._record_failure(
            "condition-1",
            "token-1",
            "not_enough_balance",
            "BUY",
        )

        self.assertEqual(first_delay, 300)
        self.assertEqual(second_delay, 900)

    async def test_risk_cancellation_suppresses_immediate_reentry(self) -> None:
        manager = OrderManager()
        accepted = PlacementResult(
            outcome="accepted",
            order_id="buy-1",
            status="live",
        )
        with patch(
            "src.order_manager.client.place_limit",
            AsyncMock(return_value=accepted),
        ):
            await manager.place_position(buy())

        with (
            patch("src.order_manager.client.cancel_order", AsyncMock(return_value=True)),
            patch("src.order_manager.client.get_order", AsyncMock(return_value=None)),
        ):
            cancelled = await manager.cancel_order("buy-1", reason="level_share")

        self.assertTrue(cancelled)
        self.assertFalse(await manager.can_place("condition-1"))
        self.assertGreater(
            await manager.placement_cooldown_remaining("condition-1"),
            250,
        )

    async def test_unreachable_open_order_list_never_marks_local_orders_missing(self) -> None:
        await db.upsert_position(buy())
        bot = FarmingBot()
        with patch(
            "src.bot.client.list_open_orders_status",
            AsyncMock(return_value=([], False)),
        ):
            result = await bot.reconcile()

        self.assertFalse(result["open_orders_reachable"])
        self.assertEqual((await db.get_position("buy-1"))["status"], "OPEN")

    async def test_stale_ambiguous_placement_is_retired_after_verified_absence(self) -> None:
        position = {
            **buy("local-ghost"),
            "status": "RECONCILE_REQUIRED",
            "placed_at": "2020-01-01T00:00:00+00:00",
            "local_id": "local-ghost",
        }
        await db.upsert_position(position)
        bot = FarmingBot()
        with (
            patch(
                "src.bot.client.list_open_orders_status",
                AsyncMock(return_value=([], True)),
            ),
            patch(
                "src.bot.client.list_positions_status",
                AsyncMock(return_value=([], True)),
            ),
        ):
            result = await bot.reconcile()

        self.assertEqual(result["marked_cancelled"], 1)
        self.assertEqual(
            (await db.get_position("local-ghost"))["status"],
            "REMOTE_ABSENT",
        )

    async def test_ambiguous_placement_is_preserved_when_tokens_exist(self) -> None:
        position = {
            **buy("local-maybe-filled"),
            "status": "RECONCILE_REQUIRED",
            "placed_at": "2020-01-01T00:00:00+00:00",
            "local_id": "local-maybe-filled",
        }
        await db.upsert_position(position)
        bot = FarmingBot()
        remote_position = SimpleNamespace(token_id="token-1", size="5")
        with (
            patch(
                "src.bot.client.list_open_orders_status",
                AsyncMock(return_value=([], True)),
            ),
            patch(
                "src.bot.client.list_positions_status",
                AsyncMock(return_value=([remote_position], True)),
            ),
        ):
            await bot.reconcile()

        self.assertEqual(
            (await db.get_position("local-maybe-filled"))["status"],
            "RECONCILE_REQUIRED",
        )

    async def test_local_journal_id_is_never_sent_to_cancel_api(self) -> None:
        position = {
            **buy("local-ghost"),
            "status": "RECONCILE_REQUIRED",
            "local_id": "local-ghost",
        }
        await db.upsert_position(position)
        manager = OrderManager()
        cancel_mock = AsyncMock()
        with patch("src.order_manager.client.cancel_order", cancel_mock):
            cancelled = await manager.cancel_order(
                "local-ghost",
                reason="manual_cancel",
            )

        self.assertFalse(cancelled)
        cancel_mock.assert_not_awaited()

    def test_http_rejection_and_transport_failure_are_distinguished(self) -> None:
        code, _message, ambiguous = _classify_order_error(
            RequestRejectedError(
                "not enough balance / allowance: the balance is not enough",
                status=400,
            )
        )
        self.assertEqual(code, "not_enough_balance")
        self.assertFalse(ambiguous)

        code, _message, ambiguous = _classify_order_error(
            TransportError("Server disconnected")
        )
        self.assertEqual(code, "transport_ambiguous")
        self.assertTrue(ambiguous)


if __name__ == "__main__":
    unittest.main()
