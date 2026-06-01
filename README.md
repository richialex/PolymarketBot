# PolymarketBot

Automated liquidity provider rewards farming bot for [Polymarket](https://polymarket.com).

## What it does

Places limit orders on Polymarket prediction markets to earn LP rewards. Targets "dead" markets with low activity and high reward-per-dollar ratio.

## Features

- Rotating market scanner — processes all reward markets in batches
- Smart scoring — `reward_per_dollar × activity_factor`
- Auto-repositioning — keeps orders inside the reward zone
- Word blacklist — filter markets by keyword
- Fixed or % based order sizing
- Web UI dashboard at `localhost:8000`

## Requirements

- Python 3.11+
- Polymarket account with API credentials

## Setup

```bash
# 1. Clone
git clone https://github.com/ggRonin/PolymarketBot.git
cd PolymarketBot

# 2. Create virtual environment
python -m venv .venv
.venv\Scripts\activate  # Windows
source .venv/bin/activate  # Linux/Mac

# 3. Install dependencies
pip install -e .

# 4. Configure
cp .env.example .env
# Edit .env with your credentials

# 5. Run
start.bat  # Windows
# or: python -m uvicorn src.main:app --port 8000
```

## Configuration

Copy `.env.example` to `.env` and fill in:

| Variable | Description |
|----------|-------------|
| `WALLET_ADDRESS` | Your Polymarket wallet address |
| `PRIVATE_KEY` | Wallet private key |
| `API_KEY` | Polymarket API key |

All bot settings are configurable via the web UI at `http://localhost:8000`.

## How rewards work

Polymarket pays LP rewards based on **number of shares** in the reward zone, not USDC value. Formula:

```
Score = ((max_spread - order_spread) / max_spread)² × order_size_in_shares
```

The bot targets markets with the highest `reward / zone_liquidity` ratio.

## Disclaimer

Use at your own risk. This bot interacts with real funds on Polymarket.
