import os
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")

    wallet_address: str = Field(alias="WALLET_ADDRESS")
    private_key: str = Field(alias="PRIVATE_KEY")
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
    min_daily_reward: float = 7.0
    depth: str = "edge"            # edge | mid | first
    category_blacklist: list[str] = ["politics"]
    volatility_threshold: float = 0.05
    min_spread: float = 2.0        # minimum rewards_max_spread in cents (±¢ from mid)
    max_ob_spread: float = 2.0     # maximum actual bid-ask spread in cents (order book quality filter)
    max_bid_depth_spread: float = 4.0  # max gap between bid levels 1 and 4 on the buy token
    max_daily_trades: int = 3      # max price changes per day — markets with more are skipped
    monitor_interval_s: int = 5   # seconds between position checks in monitor loop

    # User controls
    max_order_usdc: float = 0.0      # legacy: no longer used by the capital model
    max_positions: int = 0           # legacy: no longer used by the capital model
    word_blacklist: list[str] = []   # forbidden words in market question


settings = Settings()

# Apply proxy to environment so httpx picks it up automatically (trust_env=True)
if settings.https_proxy:
    os.environ.setdefault("HTTPS_PROXY", settings.https_proxy)
    os.environ.setdefault("ALL_PROXY",   settings.https_proxy)
if settings.http_proxy:
    os.environ.setdefault("HTTP_PROXY",  settings.http_proxy)
