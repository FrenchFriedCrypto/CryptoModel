import pandas as pd

# This is a strategy template for non-devs who can just change 
# configuration values to generate a strategy file to submit to 
# PairWise Alpha quest on Lunor Quest platform

# ========== CONFIGURATION (EDIT THIS SECTION ONLY) ==========

# 🚀 TARGET COIN to trade (this is the coin you'll generate BUY/SELL/HOLD for)
TARGET_COIN = "LDO"       # Example: "LDO", "BONK", "RAY"
TIMEFRAME = "1H"          # Timeframe of your strategy: "1H", "4H", "1D"

# 🧭 ANCHOR COINS used to derive signals (these are the coins you observe for movement)
# You MUST define each anchor coin and the timeframe of its OHLCV data
# LAG means how many candles back to look when calculating % change
ANCHORS = [
    {"symbol": "BTC", "timeframe": "1H", "lag": 4},   # use BTC 1H candles, look 4 hours back
    {"symbol": "ETH", "timeframe": "1H", "lag": 4},
    {"symbol": "ETH", "timeframe": "4H", "lag": 0}    # use latest ETH 4H candle (no lag)
]

# ✅ BUY RULES: Define what conditions must be true to trigger a BUY signal
# You can use one or more conditions. All must be true for BUY to happen.
# change_pct: positive for upward move, negative for drop
# direction: "up" for pump, "down" for dump
BUY_RULES = [
    {"symbol": "BTC", "timeframe": "1H", "lag": 4, "change_pct": 3.0, "direction": "up"},
    {"symbol": "ETH", "timeframe": "1H", "lag": 4, "change_pct": 5.0, "direction": "up"}
]

# ❌ SELL RULES: Define when to exit the position
# If ANY of these rules are true, a SELL signal is triggered
SELL_RULES = [
    {"symbol": "ETH", "timeframe": "4H", "lag": 0, "change_pct": -3.0, "direction": "down"}
]

# ========== STRATEGY ENGINE (DO NOT EDIT BELOW) ==========

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Strategy engine that applies config-driven logic to generate BUY/SELL/HOLD signals.
    """
    try:
        df = candles_target[['timestamp']].copy()
        for anchor in ANCHORS:
            col = f"close_{anchor['symbol']}_{anchor['timeframe']}"
            if col not in candles_anchor.columns:
                raise ValueError(f"Missing required column in anchor data: {col}")
            df[col] = candles_anchor[col].values

        signals = []
        for i in range(len(df)):
            buy_pass = True
            sell_pass = False

            for rule in BUY_RULES:
                col = f"close_{rule['symbol']}_{rule['timeframe']}"
                if col not in df.columns or pd.isna(df[col].iloc[i]):
                    buy_pass = False
                    break
                change = df[col].pct_change().shift(rule['lag']).iloc[i]
                if pd.isna(change):
                    buy_pass = False
                    break
                if rule['direction'] == 'up' and change <= rule['change_pct'] / 100:
                    buy_pass = False
                    break
                if rule['direction'] == 'down' and change >= rule['change_pct'] / 100:
                    buy_pass = False
                    break

            for rule in SELL_RULES:
                col = f"close_{rule['symbol']}_{rule['timeframe']}"
                if col not in df.columns or pd.isna(df[col].iloc[i]):
                    continue
                change = df[col].pct_change().shift(rule['lag']).iloc[i]
                if pd.isna(change):
                    continue
                if rule['direction'] == 'down' and change <= rule['change_pct'] / 100:
                    sell_pass = True
                if rule['direction'] == 'up' and change >= rule['change_pct'] / 100:
                    sell_pass = True

            if buy_pass:
                signals.append("BUY")
            elif sell_pass:
                signals.append("SELL")
            else:
                signals.append("HOLD")

        df['signal'] = signals
        return df[['timestamp', 'signal']]

    except Exception as e:
        raise RuntimeError(f"Strategy failed. Please review your config.\nError: {e}")

def get_coin_metadata() -> dict:
    """
    Provides metadata required by the evaluation engine to determine
    what data to load for the strategy.
    """
    return {
        "target": {
            "symbol": TARGET_COIN,
            "timeframe": TIMEFRAME
        },
        "anchors": [
            {"symbol": a["symbol"], "timeframe": a["timeframe"]} for a in ANCHORS
        ]
    }
