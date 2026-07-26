import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field
from src.keystore import decrypt_private_key


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    wallet_address: str = Field(alias="WALLET_ADDRESS")
    # Legacy PRIVATE_KEY remains supported only to make migration gradual.
    # Prefer KEYSTORE_FILE: its password is prompted for at startup and is not
    # stored in .env or passed through the process environment.
    private_key: str = Field(default="", alias="PRIVATE_KEY")
    keystore_file: str = Field(default="", alias="KEYSTORE_FILE")
    api_key: str = Field(alias="API_KEY")
    builder_code: str = Field(default="0x5fca799bf0816c0e257ed8553ca29930f19635970b92e2d7fc168d63f3731182", alias="BUILDER_CODE")

    # Optional proxy (http://user:pass@host:port  or  socks5://host:port)
    https_proxy: str = Field(default="", alias="HTTPS_PROXY")
    http_proxy: str  = Field(default="", alias="HTTP_PROXY")

    # Farming defaults (persisted in DB, overridden by UI)
    order_usdc: float = 10.0       # fixed USDC budget for one bot entry
    bot_capital_limit_usdc: float = 100.0  # max bot-managed BUY exposure
    free_balance_buffer_pct: float = 10.0  # keep this % of free USDC unused
    slot_pct: float = 25.0         # legacy: no longer used by the capital model
    max_slots: int = 4             # legacy: no longer used by the capital model
    max_slots_per_market: int = 2  # max worst orders cancelled in one rebalance
    scan_interval_s: int = 60
    scanner_mode: str = "legacy"    # legacy | multi | hybrid
    min_daily_reward: float = 7.0
    depth: str = "edge"            # edge | mid | first
    category_blacklist: list[str] = ["politics"]
    volatility_threshold: float = 0.05
    min_spread: float = 2.0        # minimum rewards_max_spread in cents (±¢ from mid)
    max_ob_spread: float = 2.0     # maximum actual bid-ask spread in cents (order book quality filter)
    max_bid_depth_spread: float = 4.0  # max gap between bid levels 1 and 4 on the buy token
    target_level_share_enabled: bool = True  # cap our share of the exact bid level
    max_target_level_share_pct: float = 50.0  # max share of exact target bid level taken by our BUY
    target_level_share_confirm_s: int = 5  # wait before shrinking/cancelling an oversized live BUY
    sell_mode: str = "maker"       # maker | market_after_delay
    market_sell_delay_s: int = 60  # wait after BUY fill before market-style SELL
    market_sell_policy: str = "always"  # always | max_gap
    market_sell_max_gap_cents: float = 4.0  # max buy-price to best-bid gap for max_gap policy
    max_daily_trades: int = 3      # max price changes per day — markets with more are skipped
    monitor_interval_s: int = 5   # seconds between position checks in monitor loop

    # Front-run protection: cancel BUY when thin level ahead is being eaten
    front_run_protection: bool = True
    front_run_bid_threshold_usd: float = 15.0   # activate only if best bid < this (USD)
    front_run_eat_pct: float = 30.0             # % of level consumed in window → trigger
    front_run_window_s: float = 3.0             # observation window (seconds)
    front_run_cooldown_s: float = 15.0          # wait before re-entry after exit

    # User controls
    max_order_usdc: float = 0.0      # legacy: no longer used by the capital model
    max_positions: int = 0           # legacy: no longer used by the capital model
    word_blacklist: list[str] = []   # forbidden words in market question


settings = Settings()

if settings.keystore_file.strip():
    settings.private_key = decrypt_private_key(settings.keystore_file)
elif not settings.private_key.strip():
    raise RuntimeError(
        "Set KEYSTORE_FILE to an encrypted keystore, or set PRIVATE_KEY temporarily "
        "while migrating. Run `python -m src.keystore create` to create one."
    )

# Apply proxy to environment so httpx picks it up automatically (trust_env=True)
if settings.https_proxy:
    os.environ.setdefault("HTTPS_PROXY", settings.https_proxy)
    os.environ.setdefault("ALL_PROXY",   settings.https_proxy)
if settings.http_proxy:
    os.environ.setdefault("HTTP_PROXY",  settings.http_proxy)
