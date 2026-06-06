from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path

import aiosqlite
import websockets

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.db import ACTIVE_POSITION_STATUSES, DB_PATH  # noqa: E402
from src.logging_setup import configure_ws_logger  # noqa: E402
from src.pm_client import client  # noqa: E402


USER_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/user"
MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Log Polymarket WS events without changing DB state.")
    parser.add_argument("--seconds", type=int, default=120, help="How long to listen before exiting.")
    parser.add_argument("--user", action="store_true", help="Probe authenticated user channel.")
    parser.add_argument("--market-ws", action="store_true", help="Probe public market channel.")
    parser.add_argument("--market", action="append", default=[], help="Condition ID for user_ws subscription.")
    parser.add_argument("--asset", action="append", default=[], help="Token/asset ID for market_ws subscription.")
    parser.add_argument("--no-db", action="store_true", help="Do not add active positions from local DB.")
    parser.add_argument("--dynamic-db", action="store_true", help="Poll DB and subscribe to new active positions while running.")
    parser.add_argument("--refresh-seconds", type=int, default=10, help="DB refresh interval for --dynamic-db.")
    return parser.parse_args()


async def active_ids_from_db() -> tuple[list[str], list[str]]:
    if not DB_PATH.exists():
        return [], []
    placeholders = ",".join("?" for _ in ACTIVE_POSITION_STATUSES)
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            f"SELECT condition_id, token_id FROM positions WHERE status IN ({placeholders})",
            ACTIVE_POSITION_STATUSES,
        ) as cursor:
            rows = await cursor.fetchall()
    markets = sorted({str(row[0] or "") for row in rows if row[0]})
    assets = sorted({str(row[1] or "") for row in rows if row[1]})
    return markets, assets


async def heartbeat(ws, stop: asyncio.Event, log, label: str) -> None:
    while not stop.is_set():
        await asyncio.sleep(10)
        if stop.is_set():
            return
        await ws.send("PING")
        log.info("%s SEND PING", label)


async def recv_loop(ws, stop: asyncio.Event, log, label: str) -> None:
    while not stop.is_set():
        try:
            msg = await asyncio.wait_for(ws.recv(), timeout=1)
        except asyncio.TimeoutError:
            continue
        log.info("%s RECV %s", label, msg)


async def user_db_subscribe_loop(ws, stop: asyncio.Event, log, subscribed: set[str], refresh_seconds: int) -> None:
    while not stop.is_set():
        await asyncio.sleep(refresh_seconds)
        if stop.is_set():
            return
        markets, _assets = await active_ids_from_db()
        current = set(markets)
        new_markets = sorted(current - subscribed)
        removed_markets = sorted(subscribed - current)
        if new_markets:
            msg = {"markets": new_markets, "operation": "subscribe"}
            await ws.send(json.dumps(msg))
            subscribed.update(new_markets)
            log.info("USER DYNAMIC SUB %s", json.dumps(msg, ensure_ascii=False))
        if removed_markets:
            log.info("USER WOULD UNSUB markets=%s", removed_markets)


async def market_db_subscribe_loop(ws, stop: asyncio.Event, log, subscribed: set[str], refresh_seconds: int) -> None:
    while not stop.is_set():
        await asyncio.sleep(refresh_seconds)
        if stop.is_set():
            return
        _markets, assets = await active_ids_from_db()
        current = set(assets)
        new_assets = sorted(current - subscribed)
        removed_assets = sorted(subscribed - current)
        if new_assets:
            msg = {"assets_ids": new_assets, "operation": "subscribe", "custom_feature_enabled": True}
            await ws.send(json.dumps(msg))
            subscribed.update(new_assets)
            log.info("MARKET DYNAMIC SUB %s", json.dumps(msg, ensure_ascii=False))
        if removed_assets:
            log.info("MARKET WOULD UNSUB assets=%s", removed_assets)


async def probe_user(markets: list[str], seconds: int, log, dynamic_db: bool, refresh_seconds: int) -> None:
    if not markets:
        log.warning("USER skip: no condition IDs. Pass --market or keep active positions in DB.")
        return

    auth = client.ws_auth()
    subscribed = set(markets)
    sub = {"auth": auth, "markets": markets, "type": "user"}
    safe_sub = {"auth": {"apiKey": auth["apiKey"], "secret": "***", "passphrase": "***"}, "markets": markets, "type": "user"}
    stop = asyncio.Event()

    log.info("USER connecting %s", USER_WS_URL)
    async with websockets.connect(USER_WS_URL, ping_interval=None) as ws:
        await ws.send(json.dumps(sub))
        log.info("USER SUB %s", json.dumps(safe_sub, ensure_ascii=False))
        tasks = [
            asyncio.create_task(heartbeat(ws, stop, log, "USER")),
            asyncio.create_task(recv_loop(ws, stop, log, "USER")),
        ]
        if dynamic_db:
            tasks.append(asyncio.create_task(user_db_subscribe_loop(ws, stop, log, subscribed, refresh_seconds)))
        await asyncio.sleep(seconds)
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def probe_market(assets: list[str], seconds: int, log, dynamic_db: bool, refresh_seconds: int) -> None:
    if not assets:
        log.warning("MARKET skip: no token IDs. Pass --asset or keep active positions in DB.")
        return

    subscribed = set(assets)
    sub = {"assets_ids": assets, "type": "market", "custom_feature_enabled": True}
    stop = asyncio.Event()

    log.info("MARKET connecting %s", MARKET_WS_URL)
    async with websockets.connect(MARKET_WS_URL, ping_interval=None) as ws:
        await ws.send(json.dumps(sub))
        log.info("MARKET SUB %s", json.dumps(sub, ensure_ascii=False))
        tasks = [
            asyncio.create_task(heartbeat(ws, stop, log, "MARKET")),
            asyncio.create_task(recv_loop(ws, stop, log, "MARKET")),
        ]
        if dynamic_db:
            tasks.append(asyncio.create_task(market_db_subscribe_loop(ws, stop, log, subscribed, refresh_seconds)))
        await asyncio.sleep(seconds)
        stop.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def main() -> None:
    args = parse_args()
    log = configure_ws_logger()

    run_user = args.user
    run_market = args.market_ws
    if not run_user and not run_market:
        run_user = True
        run_market = True

    db_markets: list[str] = []
    db_assets: list[str] = []
    if not args.no_db:
        db_markets, db_assets = await active_ids_from_db()

    markets = sorted(set(args.market + db_markets))
    assets = sorted(set(args.asset + db_assets))
    log.info(
        "Probe starting seconds=%d user=%s market_ws=%s markets=%d assets=%d dynamic_db=%s refresh=%d",
        args.seconds, run_user, run_market, len(markets), len(assets), args.dynamic_db, args.refresh_seconds,
    )

    tasks = []
    if run_user:
        tasks.append(asyncio.create_task(probe_user(markets, args.seconds, log, args.dynamic_db and not args.no_db, args.refresh_seconds)))
    if run_market:
        tasks.append(asyncio.create_task(probe_market(assets, args.seconds, log, args.dynamic_db and not args.no_db, args.refresh_seconds)))

    await asyncio.gather(*tasks)
    log.info("Probe finished")


if __name__ == "__main__":
    asyncio.run(main())
