"""Async SQLite storage for positions and runtime settings."""
from __future__ import annotations

import json
import aiosqlite
from pathlib import Path

DB_PATH = Path(__file__).parent.parent / "farm.db"
ACTIVE_POSITION_STATUSES = ("PENDING_PLACE", "OPEN", "WARNING", "SELL_PENDING", "SELL_OPEN")
CAPITAL_IN_USE_STATUSES = ("PENDING_PLACE", "OPEN", "WARNING", "FILLED")

_CREATE_POSITIONS = """
CREATE TABLE IF NOT EXISTS positions (
    order_id       TEXT PRIMARY KEY,
    condition_id   TEXT NOT NULL,
    market_question TEXT NOT NULL,
    token_id       TEXT NOT NULL,
    outcome        TEXT NOT NULL DEFAULT '',
    side           TEXT NOT NULL,
    price          REAL NOT NULL,
    size           REAL NOT NULL,
    status         TEXT NOT NULL DEFAULT 'OPEN',
    placed_at      TEXT NOT NULL,
    filled_at      TEXT,
    matched_size   REAL NOT NULL DEFAULT 0,
    reward_earned  REAL NOT NULL DEFAULT 0,
    parent_order_id TEXT,
    local_id       TEXT,
    source         TEXT NOT NULL DEFAULT 'BOT'
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

_CREATE_MARKET_BANS = """
CREATE TABLE IF NOT EXISTS market_bans (
    condition_id    TEXT PRIMARY KEY,
    market_question TEXT NOT NULL DEFAULT '',
    banned_at       TEXT NOT NULL,
    expires_at      TEXT NOT NULL
)
"""


async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(_CREATE_POSITIONS)
        await _ensure_column(db, "positions", "outcome", "TEXT NOT NULL DEFAULT ''")
        await _ensure_column(db, "positions", "parent_order_id", "TEXT")
        await _ensure_column(db, "positions", "local_id", "TEXT")
        await _ensure_column(db, "positions", "source", "TEXT NOT NULL DEFAULT 'BOT'")
        await _ensure_column(db, "positions", "matched_size", "REAL NOT NULL DEFAULT 0")
        await db.execute(_CREATE_SETTINGS)
        await db.execute(_CREATE_BALANCE_SNAPSHOTS)
        await db.execute(_CREATE_MARKET_BANS)
        await db.commit()


async def _ensure_column(db: aiosqlite.Connection, table: str, column: str, definition: str) -> None:
    async with db.execute(f"PRAGMA table_info({table})") as cur:
        rows = await cur.fetchall()
    if column not in {row[1] for row in rows}:
        await db.execute(f"ALTER TABLE {table} ADD COLUMN {column} {definition}")


# ── Positions ──────────────────────────────────────────────────────────────────

async def upsert_position(pos: dict) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO positions
               (order_id, condition_id, market_question, token_id, outcome, side,
                price, size, status, placed_at, filled_at, matched_size, reward_earned,
                parent_order_id, local_id, source)
               VALUES (:order_id,:condition_id,:market_question,:token_id,:outcome,:side,
                       :price,:size,:status,:placed_at,:filled_at,:matched_size,:reward_earned,
                       :parent_order_id,:local_id,:source)
               ON CONFLICT(order_id) DO UPDATE SET
                 outcome=excluded.outcome,
                 status=excluded.status,
                 filled_at=excluded.filled_at,
                 matched_size=excluded.matched_size,
                 reward_earned=excluded.reward_earned,
                 parent_order_id=excluded.parent_order_id,
                 local_id=excluded.local_id,
                 source=excluded.source""",
            {
                "order_id": pos["order_id"],
                "condition_id": pos["condition_id"],
                "market_question": pos["market_question"],
                "token_id": pos["token_id"],
                "outcome": pos.get("outcome", ""),
                "side": pos["side"],
                "price": pos["price"],
                "size": pos["size"],
                "status": pos.get("status", "OPEN"),
                "placed_at": pos["placed_at"],
                "filled_at": pos.get("filled_at"),
                "matched_size": pos.get("matched_size", 0),
                "reward_earned": pos.get("reward_earned", 0),
                "parent_order_id": pos.get("parent_order_id"),
                "local_id": pos.get("local_id"),
                "source": pos.get("source", "BOT"),
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


async def update_position_matched_size(order_id: str, matched_size: float) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE positions SET matched_size=? WHERE order_id=?",
            (matched_size, order_id),
        )
        await db.commit()


async def get_position(order_id: str) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM positions WHERE order_id=?", (order_id,)) as cursor:
            row = await cursor.fetchone()
            return dict(row) if row else None


async def replace_position_order_id(local_order_id: str, real_order_id: str, status: str = "OPEN") -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "UPDATE positions SET order_id=?, status=? WHERE order_id=?",
            (real_order_id, status, local_order_id),
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


async def get_active_positions() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        placeholders = ",".join("?" for _ in ACTIVE_POSITION_STATUSES)
        async with db.execute(
            f"SELECT * FROM positions WHERE status IN ({placeholders}) ORDER BY placed_at DESC",
            ACTIVE_POSITION_STATUSES,
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_reconcilable_positions() -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM positions
            WHERE status IN ('PENDING_PLACE','OPEN','WARNING','SELL_PENDING','SELL_OPEN')
            ORDER BY placed_at DESC
        """) as cursor:
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


async def get_position_history(limit: int = 10, offset: int = 0) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        placeholders = ",".join("?" for _ in ACTIVE_POSITION_STATUSES)
        async with db.execute(
            f"SELECT COUNT(*) FROM positions WHERE status NOT IN ({placeholders})",
            ACTIVE_POSITION_STATUSES,
        ) as cur:
            total = (await cur.fetchone())[0]
        async with db.execute(
            f"""
            SELECT * FROM positions
            WHERE status NOT IN ({placeholders})
            ORDER BY placed_at DESC
            LIMIT ? OFFSET ?
            """,
            (*ACTIVE_POSITION_STATUSES, limit, offset),
        ) as cursor:
            rows = await cursor.fetchall()
            return {"total": total, "items": [dict(r) for r in rows]}


async def get_position_history_for_condition(condition_id: str, exclude_order_id: str | None = None) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        params = [condition_id]
        extra = ""
        if exclude_order_id:
            extra = "AND order_id != ?"
            params.append(exclude_order_id)
        async with db.execute(
            f"""
            SELECT * FROM positions
            WHERE condition_id = ? {extra}
            ORDER BY placed_at DESC
            LIMIT 50
            """,
            params,
        ) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_filled_buys_without_sell() -> list[dict]:
    """Return FILLED BUY positions that have no corresponding SELL exit."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM positions p
            WHERE p.status = 'FILLED'
              AND p.side = 'BUY'
              AND NOT EXISTS (
                  SELECT 1 FROM positions s
                  WHERE s.side = 'SELL'
                    AND (
                      s.parent_order_id = p.order_id
                      OR (
                        s.parent_order_id IS NULL
                        AND s.token_id = p.token_id
                        AND s.status IN ('OPEN', 'WARNING', 'SELL_PENDING', 'SELL_OPEN')
                      )
                    )
                    AND s.status IN ('OPEN', 'WARNING', 'SELL_PENDING', 'SELL_OPEN', 'FILLED')
              )
            ORDER BY p.filled_at DESC
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def delete_position(order_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM positions WHERE order_id=?", (order_id,))
        await db.commit()


# ── Market bans ────────────────────────────────────────────────────────────────

async def ban_market(condition_id: str, market_question: str = "", hours: int = 24) -> dict:
    from datetime import datetime, timezone, timedelta

    now = datetime.now(timezone.utc)
    expires_at = now + timedelta(hours=max(1, int(hours)))
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            """INSERT INTO market_bans(condition_id, market_question, banned_at, expires_at)
               VALUES(?,?,?,?)
               ON CONFLICT(condition_id) DO UPDATE SET
                 market_question=excluded.market_question,
                 banned_at=excluded.banned_at,
                 expires_at=excluded.expires_at""",
            (condition_id, market_question or condition_id, now.isoformat(), expires_at.isoformat()),
        )
        await db.commit()
    return {
        "condition_id": condition_id,
        "market_question": market_question or condition_id,
        "banned_at": now.isoformat(),
        "expires_at": expires_at.isoformat(),
    }


async def prune_expired_market_bans() -> None:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM market_bans WHERE expires_at <= ?", (now,))
        await db.commit()


async def get_active_market_bans() -> dict[str, dict]:
    from datetime import datetime, timezone

    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        await db.execute("DELETE FROM market_bans WHERE expires_at <= ?", (now,))
        async with db.execute(
            "SELECT * FROM market_bans WHERE expires_at > ? ORDER BY expires_at DESC",
            (now,),
        ) as cursor:
            rows = await cursor.fetchall()
        await db.commit()
        return {r["condition_id"]: dict(r) for r in rows}


async def is_market_banned(condition_id: str) -> bool:
    bans = await get_active_market_bans()
    return condition_id in bans


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


async def get_bot_capital_in_use() -> float:
    """Return bot-managed BUY exposure still needing capital accounting."""
    async with aiosqlite.connect(DB_PATH) as db:
        placeholders = ",".join("?" for _ in CAPITAL_IN_USE_STATUSES)
        async with db.execute(
            f"""
            SELECT COALESCE(SUM(p.price * p.size), 0)
            FROM positions p
            WHERE p.side = 'BUY'
              AND p.source IN ('BOT', 'ADOPTED')
              AND p.status IN ({placeholders})
              AND NOT EXISTS (
                  SELECT 1 FROM positions s
                  WHERE s.token_id = p.token_id
                    AND s.side = 'SELL'
                    AND s.status = 'FILLED'
              )
            """,
            CAPITAL_IN_USE_STATUSES,
        ) as cur:
            row = await cur.fetchone()
            return round(float(row[0] or 0), 4)


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
