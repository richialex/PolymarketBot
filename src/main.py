"""FastAPI application — serves the UI and exposes the bot API."""
from __future__ import annotations

import logging
import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src import db
from src.bot import bot
from src.pm_client import client
from src.config import settings as cfg

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

# Always write to bot.log so we can debug even after console closes
_fh = logging.FileHandler("bot.log", encoding="utf-8")
_fh.setLevel(logging.INFO)
_fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_fh)

STATIC_DIR = Path(__file__).parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    yield
    bot.stop()


app = FastAPI(title="PolymarketFarm", lifespan=lifespan)


# ── Status ─────────────────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    try:
        balance = await asyncio.wait_for(client.get_balance(), timeout=4.0)
    except Exception:
        balance = 0
    positions = await db.get_open_positions()
    all_pos = await db.get_all_positions()
    earned = sum(p.get("reward_earned", 0) for p in all_pos)
    # Filter benign connection errors (HTTP/2 GOAWAY, timeouts)
    _noise = ("timed out", "connectionterminated", "remoteerror", "connection terminated")
    errors = [e for e in bot.errors[:10] if not any(n in e.lower() for n in _noise)]
    return {
        "running": bot.running,
        "balance": float(balance),
        "active_positions": len(positions),
        "total_earned": round(earned, 4),
        "errors": errors,
        "api_reachable": float(balance) > 0,
    }


# ── Stats ──────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    pos_stats = await db.get_position_stats()
    balance_history = await db.get_balance_history(hours=24)

    # Balance delta: current vs 24h ago
    balance_24h_ago = balance_history[0]["balance"] if balance_history else None
    try:
        current_balance = float(await asyncio.wait_for(client.get_balance(), timeout=4.0))
    except Exception:
        current_balance = 0.0
    balance_delta_24h = round(current_balance - balance_24h_ago, 4) if balance_24h_ago else None

    # Real locked USDC from Polymarket (sum of active orders)
    try:
        locked_usdc = await asyncio.wait_for(client.get_locked_usdc(), timeout=5.0)
    except Exception:
        locked_usdc = pos_stats.get("invested_usdc", 0.0)

    # Estimated daily earnings from open positions using last scan data
    candidate_map = {m.condition_id: m for m in bot.last_scan}
    open_positions = await db.get_open_positions()
    est_daily = 0.0
    for pos in open_positions:
        m = candidate_map.get(pos["condition_id"])
        if m and m.top4_liquidity_usd > 0:
            our_usdc = pos["price"] * pos["size"]
            share = our_usdc / (m.top4_liquidity_usd + our_usdc)
            est_daily += m.total_daily_rate * share

    return {
        **pos_stats,
        "invested_usdc": locked_usdc,  # override with real Polymarket value
        "current_balance": round(current_balance, 4),
        "balance_delta_24h": balance_delta_24h,
        "balance_history": balance_history[-48:],  # last 48 snapshots
        "est_daily_earnings": round(est_daily, 4),
    }


# ── Markets ────────────────────────────────────────────────────────────────────

@app.get("/api/markets")
async def get_markets():
    return [asdict(m) for m in bot.last_scan]


@app.post("/api/markets/refresh")
async def refresh_markets():
    cfg_data = await bot._load_cfg()
    from src.scanner import scan_markets
    bot.last_scan = await scan_markets(
        min_daily_reward=cfg_data["min_daily_reward"],
        max_markets=cfg_data["max_slots"],
        depth=cfg_data["depth"],
        category_blacklist=cfg_data["category_blacklist"],
        volatility_threshold=cfg_data["volatility_threshold"],
    )
    return {"count": len(bot.last_scan)}


# ── Positions ──────────────────────────────────────────────────────────────────

@app.get("/api/positions")
async def get_positions():
    return await db.get_all_positions(200)


@app.delete("/api/positions/{order_id}")
async def cancel_position(order_id: str):
    ok = await client.cancel_order(order_id)
    if ok:
        await db.update_position_status(order_id, "CANCELLED")
    return {"ok": ok}


@app.post("/api/positions/cancel_all")
async def cancel_all_positions():
    ok = await client.cancel_all()
    if ok:
        positions = await db.get_open_positions()
        for p in positions:
            await db.update_position_status(p["order_id"], "CANCELLED")
    return {"ok": ok}


# ── Bot control ────────────────────────────────────────────────────────────────

@app.post("/api/bot/start")
async def start_bot():
    bot.start()
    return {"running": True}


@app.post("/api/bot/stop")
async def stop_bot():
    bot.stop()
    return {"running": False}


@app.post("/api/bot/tick")
async def manual_tick():
    """Trigger a single scan+place cycle immediately."""
    await bot.tick()
    return {"markets_found": len(bot.last_scan)}


# ── Settings ───────────────────────────────────────────────────────────────────

class BotSettings(BaseModel):
    slot_pct: float | None = None
    max_slots_per_market: int | None = None
    scan_interval_s: int | None = None
    min_daily_reward: float | None = None
    depth: str | None = None
    category_blacklist: list[str] | None = None
    volatility_threshold: float | None = None
    min_spread: float | None = None
    max_ob_spread: float | None = None
    max_daily_trades: int | None = None
    monitor_interval_s: int | None = None
    max_order_usdc: float | None = None
    max_positions: int | None = None
    word_blacklist: list[str] | None = None


@app.get("/api/settings")
async def get_settings():
    stored = await db.get_all_settings()
    defaults = {
        "slot_pct":             cfg.slot_pct,
        "max_slots_per_market": cfg.max_slots_per_market,
        "scan_interval_s":      cfg.scan_interval_s,
        "min_daily_reward":     cfg.min_daily_reward,
        "depth":                cfg.depth,
        "category_blacklist":   cfg.category_blacklist,
        "volatility_threshold": cfg.volatility_threshold,
        "min_spread":           cfg.min_spread,
        "max_ob_spread":        cfg.max_ob_spread,
        "max_daily_trades":     cfg.max_daily_trades,
        "monitor_interval_s":   cfg.monitor_interval_s,
        "max_order_usdc":       cfg.max_order_usdc,
        "max_positions":        cfg.max_positions,
        "word_blacklist":       cfg.word_blacklist,
    }
    return {**defaults, **stored}


@app.put("/api/settings")
async def update_settings(body: BotSettings):
    data = body.model_dump(exclude_none=True)
    for k, v in data.items():
        await db.set_setting(k, v)
    return data


# ── Static UI ──────────────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))
