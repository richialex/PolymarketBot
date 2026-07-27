"""FastAPI application — serves the UI and exposes the bot API."""
from __future__ import annotations

import logging
import asyncio
import uuid
from contextlib import asynccontextmanager
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel

from src import db
from src.bot import bot
from src.pm_client import client
from src.order_manager import order_manager
from src.scanner import calc_sell_order_price, mid_from_order_book, _extract_asks
from src.config import settings as cfg
from src.logging_setup import configure_logging

configure_logging()
log = logging.getLogger(__name__)

STATIC_DIR = Path(__file__).parent.parent / "static"


@asynccontextmanager
async def lifespan(app: FastAPI):
    await db.init_db()
    try:
        result = await bot.reconcile()
        log.info("Startup reconcile: %s", result)
    except Exception as e:
        log.warning("Startup reconcile skipped: %s", e)
    yield
    bot.stop()


app = FastAPI(title="PolymarketFarm", lifespan=lifespan)


# ── Status ─────────────────────────────────────────────────────────────────────

@app.get("/api/status")
async def get_status():
    try:
        balance, api_reachable = await asyncio.wait_for(client.get_balance_status(), timeout=4.0)
    except Exception:
        balance = 0
        api_reachable = False
    positions = await db.get_active_positions()
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
        "api_reachable": api_reachable,
        "ws": await bot.ws_status(),
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

    # Cross-market BUY quotes intentionally reuse wallet collateral.  Report
    # quote notional separately from actually matched, unsold inventory.
    quote_notional = await db.get_bot_quote_notional()
    inventory_exposure = await db.get_bot_inventory_exposure()

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
        "invested_usdc": quote_notional,  # backward-compatible UI field
        "quote_notional_usdc": quote_notional,
        "inventory_exposure_usdc": inventory_exposure,
        "current_balance": round(current_balance, 4),
        "balance_delta_24h": balance_delta_24h,
        "balance_history": balance_history[-48:],  # last 48 snapshots
        "est_daily_earnings": round(est_daily, 4),
    }


# ── Markets ────────────────────────────────────────────────────────────────────

@app.get("/api/markets")
async def get_markets():
    return [asdict(m) for m in bot.shown_markets]


@app.get("/api/markets/status")
async def get_markets_status():
    return bot.scan_status


@app.post("/api/markets/refresh")
async def refresh_markets():
    cfg_data = await bot._load_cfg()
    await bot.scan_once(cfg_data)
    return {"count": len(bot.shown_markets), **bot.scan_status}


# ── Positions ──────────────────────────────────────────────────────────────────

def _serialize_order(order) -> dict:
    original_size = float(getattr(order, "original_size", 0) or 0)
    size_matched = float(getattr(order, "size_matched", 0) or 0)
    remaining_size = max(0.0, original_size - size_matched)
    price = float(getattr(order, "price", 0) or 0)
    created_at = getattr(order, "created_at", None)
    return {
        "order_id": str(getattr(order, "id", "") or ""),
        "market": str(getattr(order, "market", "") or ""),
        "token_id": str(getattr(order, "token_id", "") or ""),
        "side": str(getattr(order, "side", "") or ""),
        "outcome": str(getattr(order, "outcome", "") or ""),
        "price": price,
        "original_size": original_size,
        "size_matched": size_matched,
        "remaining_size": remaining_size,
        "remaining_usdc": round(price * remaining_size, 4),
        "status": str(getattr(order, "status", "") or ""),
        "created_at": created_at.isoformat() if created_at else None,
    }


def _serialize_unmanaged_position(pos, uncovered_size: float) -> dict:
    avg_price = float(getattr(pos, "avg_price", 0) or 0)
    cur_price = float(getattr(pos, "cur_price", 0) or 0)
    title = str(getattr(pos, "title", "") or "")
    return {
        "condition_id": str(getattr(pos, "condition_id", "") or ""),
        "market_question": title,
        "market_slug": str(getattr(pos, "slug", "") or ""),
        "token_id": str(getattr(pos, "token_id", "") or ""),
        "outcome": str(getattr(pos, "outcome", "") or ""),
        "size": float(getattr(pos, "size", 0) or 0),
        "uncovered_size": round(uncovered_size, 6),
        "avg_price": avg_price,
        "cur_price": cur_price,
        "current_value": float(getattr(pos, "current_value", 0) or 0),
        "usdc": round(uncovered_size * (cur_price or avg_price), 4),
    }


async def _unmanaged_positions() -> list[dict]:
    positions, positions_reachable = await client.list_positions_status()
    open_orders, open_orders_reachable = await client.list_open_orders_status()
    if not positions_reachable or not open_orders_reachable:
        return []
    local_positions = await db.get_reconcilable_positions()

    covered_by_token: dict[str, float] = {}
    for order in open_orders:
        if str(getattr(order, "side", "") or "").upper() != "SELL":
            continue
        token_id = str(getattr(order, "token_id", "") or "")
        original = float(getattr(order, "original_size", 0) or 0)
        matched = float(getattr(order, "size_matched", 0) or 0)
        covered_by_token[token_id] = covered_by_token.get(token_id, 0.0) + max(0.0, original - matched)

    for pos in local_positions:
        if str(pos.get("side", "") or "").upper() != "SELL":
            continue
        if str(pos.get("status", "") or "").upper() != "SELL_PENDING":
            continue
        token_id = str(pos.get("token_id", "") or "")
        covered_by_token[token_id] = covered_by_token.get(token_id, 0.0) + float(pos.get("size", 0) or 0)

    unmanaged = []
    for pos in positions:
        token_id = str(getattr(pos, "token_id", "") or "")
        size = float(getattr(pos, "size", 0) or 0)
        uncovered = size - covered_by_token.get(token_id, 0.0)
        if uncovered > 0.0001:
            unmanaged.append(_serialize_unmanaged_position(pos, uncovered))
    return unmanaged


async def _place_sell_for_unmanaged(position: dict) -> dict:
    token_id = position["token_id"]
    size = float(position["uncovered_size"])
    order_book = await client.get_order_book(token_id)
    live_mid = mid_from_order_book(order_book)
    asks = _extract_asks(order_book)
    fallback_mid = float(position.get("cur_price") or position.get("avg_price") or 0.5)
    sell_price = calc_sell_order_price(live_mid or fallback_mid, 0.04, asks)

    local_id = f"local-sell-{uuid.uuid4().hex}"
    now = datetime.now(timezone.utc).isoformat()
    resp, placed = await order_manager.place_position(
        {
            "order_id": local_id,
            "condition_id": position["condition_id"],
            "market_question": position["market_question"] or position["condition_id"],
            "token_id": token_id,
            "outcome": position.get("outcome", ""),
            "side": "SELL",
            "price": sell_price,
            "size": size,
            "status": "SELL_PENDING",
            "placed_at": now,
            "filled_at": None,
            "matched_size": 0,
            "reward_earned": 0,
            "local_id": local_id,
            "source": "ADOPTED",
        },
        pending_status="SELL_PENDING",
    )
    bot.invalidate_active_positions_cache()
    if (
        resp is None
        or bool(getattr(resp, "ambiguous", False))
        or getattr(resp, "ok", True) is False
    ):
        return {"ok": False, "order_id": None, "price": sell_price}

    order_id = str(placed.get("order_id") or "")
    if not order_id:
        return {"ok": False, "order_id": None, "price": sell_price}
    return {"ok": True, "order_id": order_id, "price": sell_price}


@app.get("/api/positions")
async def get_positions():
    return await db.get_active_positions()


@app.get("/api/positions/history")
async def get_positions_history(limit: int = 10, offset: int = 0):
    limit = max(1, min(limit, 100))
    offset = max(0, offset)
    return await db.get_position_history(limit=limit, offset=offset)


@app.get("/api/positions/history/by_condition")
async def get_positions_history_by_condition(condition_id: str, exclude_order_id: str | None = None):
    return await db.get_position_history_for_condition(condition_id, exclude_order_id)


@app.get("/api/orders/manual")
async def get_manual_orders():
    bot_positions = await db.get_open_positions()
    bot_order_ids = {p["order_id"] for p in bot_positions}
    orders, reachable = await client.list_open_orders_status()
    if not reachable:
        return []
    return [
        _serialize_order(order)
        for order in orders
        if str(getattr(order, "id", "") or "") not in bot_order_ids
    ]


@app.get("/api/orders/attempts")
async def get_order_attempts(limit: int = 100):
    return await db.get_order_attempts(limit=max(1, min(limit, 500)))


@app.get("/api/positions/unmanaged")
async def get_unmanaged_positions():
    return await _unmanaged_positions()


class UnmanagedAction(BaseModel):
    token_id: str


class MarketBanAction(BaseModel):
    condition_id: str | None = None
    market_question: str | None = None
    hours: int | None = 24


@app.post("/api/positions/unmanaged/take_control")
async def take_control_unmanaged(body: UnmanagedAction):
    positions = await _unmanaged_positions()
    pos = next((p for p in positions if p["token_id"] == body.token_id), None)
    if pos is None:
        raise HTTPException(status_code=404, detail="Unmanaged position not found")
    order_id = f"adopted-{uuid.uuid4().hex}"
    await db.upsert_position({
        "order_id": order_id,
        "condition_id": pos["condition_id"],
        "market_question": pos["market_question"] or pos["condition_id"],
        "token_id": pos["token_id"],
        "outcome": pos.get("outcome", ""),
        "side": "BUY",
        "price": pos.get("avg_price") or pos.get("cur_price") or 0,
        "size": pos["uncovered_size"],
        "status": "FILLED",
        "placed_at": datetime.now(timezone.utc).isoformat(),
        "filled_at": datetime.now(timezone.utc).isoformat(),
        "reward_earned": 0,
        "source": "ADOPTED",
    })
    bot.invalidate_active_positions_cache()
    return {"ok": True, "order_id": order_id}


@app.post("/api/positions/unmanaged/sell")
async def sell_unmanaged(body: UnmanagedAction):
    positions = await _unmanaged_positions()
    pos = next((p for p in positions if p["token_id"] == body.token_id), None)
    if pos is None:
        raise HTTPException(status_code=404, detail="Unmanaged position not found")
    return await _place_sell_for_unmanaged(pos)


@app.delete("/api/positions/{order_id}")
async def cancel_position(order_id: str):
    ok = await order_manager.cancel_order(order_id, reason="manual_cancel")
    bot.invalidate_active_positions_cache()
    return {"ok": ok}


@app.post("/api/positions/{order_id}/ban")
async def ban_position_market(order_id: str):
    pos = await db.get_position(order_id)
    if pos is None:
        raise HTTPException(status_code=404, detail="Position not found")

    ok = True
    if pos.get("status") in db.WORKING_ORDER_STATUSES:
        ok = await order_manager.cancel_order(order_id, reason="manual_ban")
        bot.invalidate_active_positions_cache()

    ban = await db.ban_market(
        pos["condition_id"],
        pos.get("market_question") or pos["condition_id"],
        hours=24,
    )
    bot.forget_market(pos["condition_id"])
    return {"ok": ok, "ban": ban}


@app.post("/api/markets/ban")
async def ban_market(body: MarketBanAction):
    if not body.condition_id:
        raise HTTPException(status_code=400, detail="condition_id is required")
    ban = await db.ban_market(
        body.condition_id,
        body.market_question or body.condition_id,
        hours=body.hours or 24,
    )
    bot.forget_market(body.condition_id)
    return {"ok": True, "ban": ban}


@app.delete("/api/orders/{order_id}")
async def cancel_order(order_id: str):
    ok = await order_manager.cancel_order(order_id, reason="manual_order_cancel")
    bot.invalidate_active_positions_cache()
    return {"ok": ok}


@app.post("/api/positions/cancel_working")
async def cancel_working_positions():
    positions = await db.get_open_positions()
    cancelled = 0
    failed = []
    for pos in positions:
        order_id = pos["order_id"]
        ok = await order_manager.cancel_order(order_id, reason="cancel_working")
        if ok:
            cancelled += 1
        else:
            failed.append(order_id)
    bot.invalidate_active_positions_cache()
    return {"ok": not failed, "cancelled": cancelled, "failed": failed}


@app.post("/api/positions/cancel_all")
async def cancel_all_positions():
    ok = await order_manager.cancel_all(reason="manual_cancel_all")
    bot.invalidate_active_positions_cache()
    return {"ok": ok}


@app.post("/api/positions/reconcile")
async def reconcile_positions():
    result = await bot.reconcile()
    bot.invalidate_active_positions_cache()
    return result


# ── Bot control ────────────────────────────────────────────────────────────────

@app.post("/api/bot/start")
async def start_bot():
    await bot.reconcile()
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
    return {"markets_found": len(bot.shown_markets)}


# ── Settings ───────────────────────────────────────────────────────────────────

class BotSettings(BaseModel):
    order_usdc: float | None = None
    bot_capital_limit_usdc: float | None = None
    free_balance_buffer_pct: float | None = None
    slot_pct: float | None = None
    max_slots_per_market: int | None = None
    scan_interval_s: int | None = None
    scanner_mode: str | None = None
    farm_mode: str | None = None
    both_scan_mode: str | None = None
    auto_probe_usdc: float | None = None
    auto_min_reward_share_pct: float | None = None
    auto_target_reward_share_pct: float | None = None
    auto_step_usdc: float | None = None
    auto_reward_check_interval_s: int | None = None
    auto_low_share_confirmations: int | None = None
    auto_reject_cooldown_s: int | None = None
    min_daily_reward: float | None = None
    depth: str | None = None
    category_blacklist: list[str] | None = None
    volatility_threshold: float | None = None
    min_spread: float | None = None
    max_ob_spread: float | None = None
    max_bid_depth_spread: float | None = None
    target_level_share_enabled: bool | None = None
    max_target_level_share_pct: float | None = None
    target_level_share_confirm_s: int | None = None
    sell_mode: str | None = None
    market_sell_delay_s: int | None = None
    market_sell_policy: str | None = None
    market_sell_max_gap_cents: float | None = None
    max_daily_trades: int | None = None
    monitor_interval_s: int | None = None
    candidate_drop_confirm_scans: int | None = None
    front_run_protection: bool | None = None
    front_run_bid_threshold_usd: float | None = None
    front_run_eat_pct: float | None = None
    front_run_window_s: float | None = None
    front_run_cooldown_s: float | None = None
    max_order_usdc: float | None = None
    max_positions: int | None = None
    word_blacklist: list[str] | None = None


@app.get("/api/settings")
async def get_settings():
    stored = await db.get_all_settings()
    legacy_order_default = stored.get("max_order_usdc", cfg.max_order_usdc)
    if not legacy_order_default:
        legacy_order_default = cfg.order_usdc
    defaults = {
        "order_usdc":           cfg.order_usdc,
        "bot_capital_limit_usdc": cfg.bot_capital_limit_usdc,
        "free_balance_buffer_pct": cfg.free_balance_buffer_pct,
        "slot_pct":             cfg.slot_pct,
        "max_slots_per_market": cfg.max_slots_per_market,
        "scan_interval_s":      cfg.scan_interval_s,
        "scanner_mode":         cfg.scanner_mode,
        "farm_mode":            cfg.farm_mode,
        "both_scan_mode":       cfg.both_scan_mode,
        "auto_probe_usdc":      cfg.auto_probe_usdc,
        "auto_min_reward_share_pct": cfg.auto_min_reward_share_pct,
        "auto_target_reward_share_pct": cfg.auto_target_reward_share_pct,
        "auto_step_usdc":       cfg.auto_step_usdc,
        "auto_reward_check_interval_s": cfg.auto_reward_check_interval_s,
        "auto_low_share_confirmations": cfg.auto_low_share_confirmations,
        "auto_reject_cooldown_s": cfg.auto_reject_cooldown_s,
        "min_daily_reward":     cfg.min_daily_reward,
        "depth":                cfg.depth,
        "category_blacklist":   cfg.category_blacklist,
        "volatility_threshold": cfg.volatility_threshold,
        "min_spread":           cfg.min_spread,
        "max_ob_spread":        cfg.max_ob_spread,
        "max_bid_depth_spread": cfg.max_bid_depth_spread,
        "target_level_share_enabled": cfg.target_level_share_enabled,
        "max_target_level_share_pct": cfg.max_target_level_share_pct,
        "target_level_share_confirm_s": cfg.target_level_share_confirm_s,
        "sell_mode":            cfg.sell_mode,
        "market_sell_delay_s":  cfg.market_sell_delay_s,
        "market_sell_policy":   cfg.market_sell_policy,
        "market_sell_max_gap_cents": cfg.market_sell_max_gap_cents,
        "max_daily_trades":     cfg.max_daily_trades,
        "monitor_interval_s":   cfg.monitor_interval_s,
        "candidate_drop_confirm_scans": cfg.candidate_drop_confirm_scans,
        "front_run_protection": cfg.front_run_protection,
        "front_run_bid_threshold_usd": cfg.front_run_bid_threshold_usd,
        "front_run_eat_pct":    cfg.front_run_eat_pct,
        "front_run_window_s":   cfg.front_run_window_s,
        "front_run_cooldown_s": cfg.front_run_cooldown_s,
        "max_order_usdc":       cfg.max_order_usdc,
        "max_positions":        cfg.max_positions,
        "word_blacklist":       cfg.word_blacklist,
    }
    if "order_usdc" not in stored:
        defaults["order_usdc"] = legacy_order_default
    return {**defaults, **stored}


@app.put("/api/settings")
async def update_settings(body: BotSettings):
    data = body.model_dump(exclude_none=True)
    if "scanner_mode" in data:
        mode = str(data["scanner_mode"] or "legacy").lower()
        data["scanner_mode"] = mode if mode in ("legacy", "multi", "hybrid") else "legacy"
    if "farm_mode" in data:
        mode = str(data["farm_mode"] or "cheap").lower()
        data["farm_mode"] = mode if mode in ("cheap", "expensive", "both", "auto") else "cheap"
    if "both_scan_mode" in data:
        mode = str(data["both_scan_mode"] or "cheap").lower()
        data["both_scan_mode"] = mode if mode in ("cheap", "strict") else "cheap"
    if "order_usdc" in data:
        data["order_usdc"] = max(0.0, float(data["order_usdc"]))
    if "auto_probe_usdc" in data:
        data["auto_probe_usdc"] = max(0.01, float(data["auto_probe_usdc"]))
    if "auto_min_reward_share_pct" in data:
        data["auto_min_reward_share_pct"] = max(0.0, float(data["auto_min_reward_share_pct"]))
    if "auto_target_reward_share_pct" in data:
        data["auto_target_reward_share_pct"] = max(0.0, float(data["auto_target_reward_share_pct"]))
    if "auto_step_usdc" in data:
        data["auto_step_usdc"] = max(0.01, float(data["auto_step_usdc"]))
    if "auto_reward_check_interval_s" in data:
        data["auto_reward_check_interval_s"] = max(60, int(data["auto_reward_check_interval_s"]))
    if "auto_low_share_confirmations" in data:
        data["auto_low_share_confirmations"] = max(1, int(data["auto_low_share_confirmations"]))
    if "auto_reject_cooldown_s" in data:
        data["auto_reject_cooldown_s"] = max(60, int(data["auto_reject_cooldown_s"]))
    if "bot_capital_limit_usdc" in data:
        data["bot_capital_limit_usdc"] = max(0.0, float(data["bot_capital_limit_usdc"]))
    if "free_balance_buffer_pct" in data:
        data["free_balance_buffer_pct"] = min(100.0, max(0.0, float(data["free_balance_buffer_pct"])))
    if "max_target_level_share_pct" in data:
        data["max_target_level_share_pct"] = min(99.0, max(0.0, float(data["max_target_level_share_pct"])))
    if "target_level_share_confirm_s" in data:
        data["target_level_share_confirm_s"] = max(0, int(data["target_level_share_confirm_s"]))
    if "sell_mode" in data:
        mode = str(data["sell_mode"] or "maker").lower()
        data["sell_mode"] = mode if mode in ("maker", "market_after_delay") else "maker"
    if "market_sell_delay_s" in data:
        data["market_sell_delay_s"] = max(0, int(data["market_sell_delay_s"]))
    if "market_sell_policy" in data:
        policy = str(data["market_sell_policy"] or "always").lower()
        data["market_sell_policy"] = policy if policy in ("always", "max_gap") else "always"
    if "market_sell_max_gap_cents" in data:
        data["market_sell_max_gap_cents"] = max(0.0, float(data["market_sell_max_gap_cents"]))
    if "front_run_protection" in data:
        data["front_run_protection"] = bool(data["front_run_protection"])
    if "candidate_drop_confirm_scans" in data:
        data["candidate_drop_confirm_scans"] = max(
            1,
            int(data["candidate_drop_confirm_scans"]),
        )
    if "front_run_bid_threshold_usd" in data:
        data["front_run_bid_threshold_usd"] = max(0.0, float(data["front_run_bid_threshold_usd"]))
    if "front_run_eat_pct" in data:
        data["front_run_eat_pct"] = min(100.0, max(1.0, float(data["front_run_eat_pct"])))
    if "front_run_window_s" in data:
        data["front_run_window_s"] = max(0.1, float(data["front_run_window_s"]))
    if "front_run_cooldown_s" in data:
        data["front_run_cooldown_s"] = max(0.0, float(data["front_run_cooldown_s"]))
    for k, v in data.items():
        await db.set_setting(k, v)
    bot.update_runtime_settings(data)
    return data


# ── Static UI ──────────────────────────────────────────────────────────────────

app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
async def index():
    return FileResponse(str(STATIC_DIR / "index.html"))
