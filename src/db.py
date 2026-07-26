"""Async SQLite storage for positions and runtime settings."""
from __future__ import annotations

import json
import aiosqlite
from pathlib import Path
from datetime import datetime, timezone

DB_PATH = Path(__file__).parent.parent / "farm.db"
WORKING_ORDER_STATUSES = (
    "PENDING_PLACE",
    "OPEN",
    "WARNING",
    "PARTIALLY_FILLED",
    "CANCEL_PENDING",
    "RECONCILE_REQUIRED",
    "SELL_PENDING",
    "SELL_OPEN",
)
LIVE_ORDER_STATUSES = ("OPEN", "WARNING", "PARTIALLY_FILLED", "SELL_OPEN")
ACTIVE_POSITION_STATUSES = WORKING_ORDER_STATUSES + ("EXIT_REQUIRED", "EXITING")
QUOTE_NOTIONAL_STATUSES = WORKING_ORDER_STATUSES

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

_CREATE_ORDER_FILLS = """
CREATE TABLE IF NOT EXISTS order_fills (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id        TEXT NOT NULL,
    condition_id    TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    side            TEXT NOT NULL,
    market_question TEXT NOT NULL DEFAULT '',
    last_price      REAL NOT NULL DEFAULT 0,
    last_size       REAL NOT NULL DEFAULT 0,
    delta_size      REAL NOT NULL,
    cumulative_size REAL NOT NULL,
    price           REAL NOT NULL,
    event_at        TEXT NOT NULL,
    source          TEXT NOT NULL DEFAULT 'WS',
    UNIQUE(order_id, cumulative_size)
)
"""

_CREATE_ORDER_ATTEMPTS = """
CREATE TABLE IF NOT EXISTS order_attempts (
    condition_id    TEXT NOT NULL,
    token_id        TEXT NOT NULL,
    side            TEXT NOT NULL,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    accepted_count  INTEGER NOT NULL DEFAULT 0,
    rejected_count  INTEGER NOT NULL DEFAULT 0,
    ambiguous_count INTEGER NOT NULL DEFAULT 0,
    consecutive_failures INTEGER NOT NULL DEFAULT 0,
    last_local_id   TEXT,
    last_order_id   TEXT,
    last_outcome    TEXT NOT NULL DEFAULT '',
    last_error_code TEXT NOT NULL DEFAULT '',
    last_error_message TEXT NOT NULL DEFAULT '',
    first_attempt_at TEXT NOT NULL,
    last_attempt_at TEXT NOT NULL,
    next_retry_at   TEXT,
    PRIMARY KEY(condition_id, token_id, side)
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
        await db.execute(_CREATE_ORDER_FILLS)
        await db.execute(_CREATE_ORDER_ATTEMPTS)
        await _ensure_column(
            db,
            "order_attempts",
            "consecutive_failures",
            "INTEGER NOT NULL DEFAULT 0",
        )
        await _ensure_column(db, "order_attempts", "market_question", "TEXT NOT NULL DEFAULT ''")
        await _ensure_column(db, "order_attempts", "last_price", "REAL NOT NULL DEFAULT 0")
        await _ensure_column(db, "order_attempts", "last_size", "REAL NOT NULL DEFAULT 0")
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_positions_status_side ON positions(status, side)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_positions_condition ON positions(condition_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_positions_token ON positions(token_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_positions_parent ON positions(parent_order_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_order_fills_order ON order_fills(order_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_order_fills_token ON order_fills(token_id)"
        )
        await db.execute(
            "CREATE INDEX IF NOT EXISTS idx_order_attempts_retry ON order_attempts(next_retry_at)"
        )
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


async def record_cumulative_match(
    order_id: str,
    cumulative_size: float,
    *,
    source: str = "WS",
    event_at: str | None = None,
) -> dict | None:
    """Atomically persist a cumulative match update and its positive delta.

    WebSocket order updates report cumulative ``size_matched``.  This function
    makes duplicate/out-of-order events harmless and is the single source of
    truth for fill deltas used by inventory/exit handling.
    """
    cumulative_size = max(0.0, float(cumulative_size or 0))
    event_at = event_at or datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        await conn.execute("BEGIN IMMEDIATE")
        async with conn.execute(
            "SELECT * FROM positions WHERE order_id=?",
            (order_id,),
        ) as cursor:
            row = await cursor.fetchone()
        if row is None:
            await conn.rollback()
            return None

        pos = dict(row)
        previous = max(0.0, float(pos.get("matched_size") or 0))
        size = max(0.0, float(pos.get("size") or 0))
        effective = min(size, cumulative_size) if size > 0 else cumulative_size
        delta = max(0.0, effective - previous)
        stored_match = max(previous, effective)

        current_status = str(pos.get("status") or "").upper()
        if size > 0 and stored_match >= size - 0.0001:
            new_status = "FILLED"
        elif stored_match > 0 and current_status not in ("EXIT_REQUIRED", "EXITING"):
            new_status = "PARTIALLY_FILLED"
        else:
            new_status = current_status

        if delta > 0:
            await conn.execute(
                """
                INSERT OR IGNORE INTO order_fills
                    (order_id, condition_id, token_id, side, delta_size,
                     cumulative_size, price, event_at, source)
                VALUES(?,?,?,?,?,?,?,?,?)
                """,
                (
                    order_id,
                    pos["condition_id"],
                    pos["token_id"],
                    str(pos["side"]).upper(),
                    delta,
                    effective,
                    float(pos["price"]),
                    event_at,
                    source,
                ),
            )

        await conn.execute(
            "UPDATE positions SET matched_size=?, status=?, filled_at=? WHERE order_id=?",
            (
                stored_match,
                new_status,
                event_at if new_status == "FILLED" else pos.get("filled_at"),
                order_id,
            ),
        )
        await conn.commit()
        pos.update(
            {
                "matched_size": stored_match,
                "status": new_status,
                "fill_delta": delta,
            }
        )
        return pos


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
        placeholders = ",".join("?" for _ in LIVE_ORDER_STATUSES)
        async with db.execute(
            f"SELECT * FROM positions WHERE status IN ({placeholders}) ORDER BY placed_at DESC",
            LIVE_ORDER_STATUSES,
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
        placeholders = ",".join("?" for _ in WORKING_ORDER_STATUSES)
        async with db.execute(
            f"""
            SELECT * FROM positions
            WHERE status IN ({placeholders})
            ORDER BY placed_at DESC
            """,
            WORKING_ORDER_STATUSES,
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
    """Return BUY executions that still need an exit order."""
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("""
            SELECT * FROM positions p
            WHERE p.status IN ('FILLED', 'EXIT_REQUIRED', 'EXITING', 'PARTIALLY_FILLED')
              AND p.side = 'BUY'
              AND COALESCE(NULLIF(p.matched_size, 0), p.size) > (
                  SELECT COALESCE(SUM(
                      CASE
                        WHEN s.status = 'FILLED' AND s.matched_size = 0 THEN s.size
                        ELSE s.matched_size
                      END
                  ), 0)
                  FROM positions s
                  WHERE s.side = 'SELL'
                    AND s.parent_order_id = p.order_id
              )
              AND NOT EXISTS (
                  SELECT 1 FROM positions s
                  WHERE s.side = 'SELL'
                    AND s.parent_order_id = p.order_id
                    AND s.status IN (
                        'PENDING_PLACE','OPEN','WARNING','PARTIALLY_FILLED',
                        'CANCEL_PENDING','RECONCILE_REQUIRED','SELL_PENDING','SELL_OPEN'
                    )
              )
            ORDER BY p.filled_at DESC
        """) as cursor:
            rows = await cursor.fetchall()
            return [dict(r) for r in rows]


async def get_parent_exit_accounting(parent_order_id: str) -> dict:
    """Return acquired, sold, working-sell, and uncovered shares for one BUY."""
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM positions WHERE order_id=?",
            (parent_order_id,),
        ) as cursor:
            buy = await cursor.fetchone()
        if buy is None:
            return {
                "acquired": 0.0,
                "sold": 0.0,
                "working_sell": 0.0,
                "uncovered": 0.0,
            }

        buy_d = dict(buy)
        acquired = float(buy_d.get("matched_size") or 0)
        if acquired <= 0 and str(buy_d.get("status") or "").upper() == "FILLED":
            acquired = float(buy_d.get("size") or 0)

        placeholders = ",".join("?" for _ in WORKING_ORDER_STATUSES)
        params = (parent_order_id, *WORKING_ORDER_STATUSES)
        async with conn.execute(
            f"""
            SELECT
              COALESCE(SUM(
                CASE
                  WHEN status='FILLED' AND matched_size=0 THEN size
                  ELSE matched_size
                END
              ), 0) AS sold,
              COALESCE(SUM(
                CASE
                  WHEN status IN ({placeholders})
                  THEN MAX(0, size-matched_size)
                  ELSE 0
                END
              ), 0) AS working_sell
            FROM positions
            WHERE parent_order_id=? AND side='SELL'
            """,
            (*WORKING_ORDER_STATUSES, parent_order_id),
        ) as cursor:
            row = await cursor.fetchone()
        sold = float(row["sold"] or 0)
        working = float(row["working_sell"] or 0)
        return {
            "acquired": acquired,
            "sold": sold,
            "working_sell": working,
            "uncovered": max(0.0, acquired - sold - working),
        }


async def get_working_sell_remaining(token_id: str) -> float:
    async with aiosqlite.connect(DB_PATH) as conn:
        placeholders = ",".join("?" for _ in WORKING_ORDER_STATUSES)
        async with conn.execute(
            f"""
            SELECT COALESCE(SUM(MAX(0, size-matched_size)), 0)
            FROM positions
            WHERE token_id=? AND side='SELL' AND status IN ({placeholders})
            """,
            (token_id, *WORKING_ORDER_STATUSES),
        ) as cursor:
            row = await cursor.fetchone()
            return max(0.0, float(row[0] or 0))


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
    """Backward-compatible alias for nominal open BUY quotes.

    Cross-market quotes may intentionally reuse the same wallet collateral, so
    this value is *not* treated as globally locked cash.
    """
    return await get_bot_quote_notional()


async def get_bot_quote_notional() -> float:
    """Return remaining nominal value of all working bot BUY quotes."""
    async with aiosqlite.connect(DB_PATH) as db:
        placeholders = ",".join("?" for _ in QUOTE_NOTIONAL_STATUSES)
        async with db.execute(
            f"""
            SELECT COALESCE(SUM(p.price * MAX(0, p.size-p.matched_size)), 0)
            FROM positions p
            WHERE p.side = 'BUY'
              AND p.source IN ('BOT', 'ADOPTED')
              AND p.status IN ({placeholders})
            """,
            QUOTE_NOTIONAL_STATUSES,
        ) as cur:
            row = await cur.fetchone()
            return round(float(row[0] or 0), 4)


async def get_bot_inventory_exposure() -> float:
    """Return cost-basis exposure of matched BUY shares not yet sold."""
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            """
            SELECT COALESCE(SUM(
              p.price * MAX(
                0,
                (CASE
                   WHEN p.matched_size > 0 THEN p.matched_size
                   WHEN p.status='FILLED' THEN p.size
                   ELSE 0
                 END)
                -
                (SELECT COALESCE(SUM(
                   CASE
                     WHEN s.status='FILLED' AND s.matched_size=0 THEN s.size
                     ELSE s.matched_size
                   END
                 ), 0)
                 FROM positions s
                 WHERE s.parent_order_id=p.order_id AND s.side='SELL')
              )
            ), 0)
            FROM positions p
            WHERE p.side='BUY'
              AND p.source IN ('BOT','ADOPTED')
            """
        ) as cursor:
            row = await cursor.fetchone()
            return round(float(row[0] or 0), 4)


# ── Placement attempts ────────────────────────────────────────────────────────

async def begin_order_attempt(pos: dict) -> None:
    """Increment one aggregate attempt row before the non-idempotent POST."""
    now = datetime.now(timezone.utc).isoformat()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            """
            INSERT INTO order_attempts (
                condition_id, token_id, side, market_question, last_price, last_size,
                attempt_count,
                last_local_id, last_outcome, first_attempt_at, last_attempt_at
            ) VALUES (?, ?, ?, ?, ?, ?, 1, ?, 'PENDING', ?, ?)
            ON CONFLICT(condition_id, token_id, side) DO UPDATE SET
                attempt_count=order_attempts.attempt_count+1,
                market_question=excluded.market_question,
                last_price=excluded.last_price,
                last_size=excluded.last_size,
                last_local_id=excluded.last_local_id,
                last_outcome='PENDING',
                last_error_code='',
                last_error_message='',
                last_attempt_at=excluded.last_attempt_at
            """,
            (
                str(pos.get("condition_id") or ""),
                str(pos.get("token_id") or ""),
                str(pos.get("side") or "").upper(),
                str(pos.get("market_question") or ""),
                float(pos.get("price") or 0),
                float(pos.get("size") or 0),
                str(pos.get("order_id") or ""),
                now,
                now,
            ),
        )
        await conn.commit()


async def finish_order_attempt(
    pos: dict,
    *,
    outcome: str,
    error_code: str = "",
    error_message: str = "",
    remote_order_id: str = "",
    retry_delay_s: float = 0.0,
) -> None:
    """Finish the latest attempt without creating another positions row."""
    from datetime import timedelta

    now = datetime.now(timezone.utc)
    next_retry_at = (
        now + timedelta(seconds=float(retry_delay_s))
    ).isoformat() if retry_delay_s > 0 else None
    outcome_upper = str(outcome or "").upper()
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            """
            UPDATE order_attempts
            SET accepted_count=accepted_count+?,
                rejected_count=rejected_count+?,
                ambiguous_count=ambiguous_count+?,
                consecutive_failures=CASE
                    WHEN ?='ACCEPTED' THEN 0
                    WHEN ?='REJECTED' THEN consecutive_failures+1
                    ELSE consecutive_failures
                END,
                last_order_id=?,
                last_outcome=?,
                last_error_code=?,
                last_error_message=?,
                last_attempt_at=?,
                next_retry_at=?
            WHERE condition_id=? AND token_id=? AND side=?
            """,
            (
                1 if outcome_upper == "ACCEPTED" else 0,
                1 if outcome_upper == "REJECTED" else 0,
                1 if outcome_upper == "AMBIGUOUS" else 0,
                outcome_upper,
                outcome_upper,
                remote_order_id,
                outcome_upper,
                error_code,
                error_message[:1000],
                now.isoformat(),
                next_retry_at,
                str(pos.get("condition_id") or ""),
                str(pos.get("token_id") or ""),
                str(pos.get("side") or "").upper(),
            ),
        )
        await conn.commit()


async def get_order_consecutive_failures(
    condition_id: str,
    token_id: str,
    side: str,
) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            """
            SELECT consecutive_failures
            FROM order_attempts
            WHERE condition_id=? AND token_id=? AND side=?
            """,
            (condition_id, token_id, str(side).upper()),
        ) as cursor:
            row = await cursor.fetchone()
            return max(0, int(row[0] or 0)) if row else 0


async def get_condition_retry_delay(condition_id: str) -> float:
    """Return persisted seconds until any route in the condition may retry."""
    now = datetime.now(timezone.utc)
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            """
            SELECT MAX(next_retry_at)
            FROM order_attempts
            WHERE condition_id=? AND next_retry_at IS NOT NULL
            """,
            (condition_id,),
        ) as cursor:
            row = await cursor.fetchone()
    if not row or not row[0]:
        return 0.0
    try:
        retry_at = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - now).total_seconds())
    except ValueError:
        return 0.0


async def get_order_retry_delay(condition_id: str, token_id: str, side: str) -> float:
    """Return persisted retry delay for one exact placement route."""
    now = datetime.now(timezone.utc)
    async with aiosqlite.connect(DB_PATH) as conn:
        async with conn.execute(
            """
            SELECT next_retry_at
            FROM order_attempts
            WHERE condition_id=? AND token_id=? AND side=?
            """,
            (condition_id, token_id, str(side).upper()),
        ) as cursor:
            row = await cursor.fetchone()
    if not row or not row[0]:
        return 0.0
    try:
        retry_at = datetime.fromisoformat(str(row[0]).replace("Z", "+00:00"))
        if retry_at.tzinfo is None:
            retry_at = retry_at.replace(tzinfo=timezone.utc)
        return max(0.0, (retry_at - now).total_seconds())
    except ValueError:
        return 0.0


async def set_condition_retry_delay(
    condition_id: str,
    delay_s: float,
    *,
    side: str | None = None,
) -> None:
    """Persist a condition-level suppression after a risk-driven cancellation."""
    from datetime import timedelta

    retry_at = (
        datetime.now(timezone.utc) + timedelta(seconds=max(0.0, float(delay_s)))
    ).isoformat()
    side_filter = " AND side=?" if side else ""
    params: tuple = (retry_at, retry_at, condition_id)
    if side:
        params = (*params, str(side).upper())
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            f"""
            UPDATE order_attempts
            SET next_retry_at = CASE
                    WHEN next_retry_at IS NULL OR next_retry_at < ? THEN ?
                    ELSE next_retry_at
                END,
                last_error_code='cancel_suppression',
                last_error_message='Risk-driven cancellation cooldown'
            WHERE condition_id=?{side_filter}
            """,
            params,
        )
        await conn.commit()


async def get_order_attempts(limit: int = 100) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM order_attempts ORDER BY last_attempt_at DESC LIMIT ?",
            (max(1, int(limit)),),
        ) as cursor:
            return [dict(row) for row in await cursor.fetchall()]


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
