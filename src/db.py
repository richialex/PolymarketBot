"""Async SQLite storage for positions and runtime settings."""
from __future__ import annotations

import json
import aiosqlite
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "farm.db"

_CREATE_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    order_id       TEXT PRIMARY KEY,
    condition_id   TEXT NOT NULL,
    market_question TEXT NOT NULL,
    token_id       TEXT NOT NULL,
    side           TEXT NOT NULL,
    price          REAL NOT NULL,
    size           REAL NOT NULL,
    status         TEXT NOT NULL DEFAULT 'OPEN',
    placed_at      TEXT NOT NULL,
    filled_at      TEXT,
    reward_earned  REAL NOT NULL DEFAULT 0
)
"""

_CREATE_SETTINGS = """
CREATE TABLE IF NOT EXISTS settings (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
)
"""

_CREATE_BALANCE_SNAPSHOTS = """
CREATE TABLE IF NOT EXISTS balance_snapshots (
    ts      TEXT PRIMARY KEY,
    balance REAL NOT NULL
)
"""


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(_CREATE_POSITIONS)
        await db.execute(_CREATE_SETTINGS)
        await db.execute(_CREATE_BALANCE_SNAPSHOTS)
        await db.commit()


# ── Positions ──────────────────────────────────────────────────────────────────

async def upsert_position(pos: dict) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO positions
               (order_id, condition_id, market_question, token_id, side,
                price, size, status, placed_at, filled_at, reward_earned)
               VALUES (:order_id,:condition_id,:market_question,:token_id,:side,
                       :price,:size,:status,:placed_at,:filled_at,:reward_earned)
               ON CONFLICT(order_id) DO UPDATE SET
                 status=excluded.status,
                 filled_at=excluded.filled_at,
                 reward_earned=excluded.reward_earned""",
            {
                "order_id": pos["order_id"],
                "condition_id": pos["condition_id"],
                "market_question": pos["market_question"],
                "token_id": pos["token_id"],
                "side": pos["side"],
                "price": pos["price"],
                "size": pos["size"],
                "status": pos.get("status", "OPEN"),
                "placed_at": pos["placed_at"],
                "filled_at": pos.get("filled_at"),
                "reward_earned": pos.get("reward_earned", 0),
            },
        )
        await db.commit()


async def update_position_status(order_id: str, status: str, filled_at: str | None = None) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE positions SET status=?, filled_at=? WHERE order_id=?",
            (status, filled_at, order_id),
        )
        await db.commit()


async def get_open_positions() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM positions WHERE status IN ('OPEN','WARNING') ORDER BY placed_at DESC"
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_all_positions(limit: int = 100) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(
            "SELECT * FROM positions ORDER BY placed_at DESC LIMIT ?", (limit,)
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_filled_buys_without_sell() -> list[dict]:
    """Return FILLED BUY positions that have no corresponding OPEN SELL for same token."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM positions p
            WHERE p.status = 'FILLED'
              AND p.side = 'BUY'
              AND NOT EXISTS (
                  SELECT 1 FROM positions s
                  WHERE s.token_id = p.token_id
                    AND s.side = 'SELL'
                    AND s.status IN ('OPEN', 'WARNING')
              )
            ORDER BY p.filled_at DESC
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def delete_position(order_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM positions WHERE order_id=?", (order_id,))
        await db.commit()


# ── Balance snapshots ─────────────────────────────────────────────────────────

async def record_balance(balance: float) -> None:
    from datetime import datetime, timezone
    ts = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO balance_snapshots(ts, balance) VALUES(?,?)",
            (ts[:16], round(balance, 4)),  # minute-level granularity to avoid spam
        )
        await db.commit()


async def get_balance_history(hours: int = 24) -> list[dict]:
    from datetime import datetime, timezone, timedelta
    since = (datetime.now(timezone.utc) - timedelta(hours=hours)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT ts, balance FROM balance_snapshots WHERE ts >= ? ORDER BY ts",
            (since,),
        ) as cur:
            rows = await cur.fetchall()
            return [{"ts": r[0], "balance": r[1]} for r in rows]


async def get_position_stats() -> dict:
    """Aggregate statistics across all positions."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status='OPEN' THEN 1 ELSE 0 END) as open,
                SUM(CASE WHEN status='FILLED' THEN 1 ELSE 0 END) as filled,
                SUM(CASE WHEN status='CANCELLED' THEN 1 ELSE 0 END) as cancelled,
                SUM(CASE WHEN status='OPEN' THEN price*size ELSE 0 END) as invested_usdc,
                SUM(reward_earned) as total_earned,
                MIN(placed_at) as first_placed
            FROM positions
        """) as cur:
            row = await cur.fetchone()
            return {
                "total": row[0] or 0,
                "open": row[1] or 0,
                "filled": row[2] or 0,
                "cancelled": row[3] or 0,
                "invested_usdc": round(row[4] or 0, 4),
                "total_earned": round(row[5] or 0, 4),
                "first_placed": row[6],
            }


# ── Settings ───────────────────────────────────────────────────────────────────

async def get_setting(key: str, default=None):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
            if row is None:
                return default
            return json.loads(row[0])


async def set_setting(key: str, value) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO settings(key,value) VALUES(?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (key, json.dumps(value)),
        )
        await db.commit()


async def get_all_settings() -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT key, value FROM settings") as cur:
            rows = await cur.fetchall()
            return {r["key"]: json.loads(r["value"]) for r in rows}
