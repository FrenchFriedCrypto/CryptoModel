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



# 1. Helper to load a CSV with a 'timestamp' and 'close' column
def load_candles(symbol: str, timeframe: str) -> pd.DataFrame:
    # 1. Read raw CSV (don’t ask pandas to parse dates)
    df = pd.read_csv(f"Data/{symbol}_{timeframe}.csv")
    # 2. Convert your millisecond timestamps into pandas Timestamps
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

# 2. Load target + anchor data
meta = get_coin_metadata()
df_target = load_candles(meta['target']['symbol'], meta['target']['timeframe'])

# build an anchor-DataFrame with all the close_X_Y columns
df_anchor = pd.DataFrame({'timestamp': df_target['timestamp']})
for a in meta['anchors']:
    df = load_candles(a['symbol'], a['timeframe'])
    col_name = f"close_{a['symbol']}_{a['timeframe']}"
    df_anchor = df_anchor.merge(
        df[['timestamp', 'close']].rename(columns={'close': col_name}),
        on='timestamp', how='left'
    )

# 3. Generate your signals
signals = generate_signals(df_target, df_anchor)   # returns timestamp + signal

# 4. Merge signals back onto the target OHLC
df = df_target.merge(signals, on='timestamp', how='left').fillna({'signal':'HOLD'})

# 5. Backtest loop
initial_cash = 10_000
cash = initial_cash
position = 0.0
trades = []

for i, row in df.iterrows():
    sig = row['signal']
    price = row['open']  # assume you trade on next candle’s open
    if sig == 'BUY' and position == 0:
        position = cash / price
        cash = 0
        trades.append({'type':'BUY','time':row['timestamp'],'price':price})
    elif sig == 'SELL' and position > 0:
        cash = position * price
        trades.append({'type':'SELL','time':row['timestamp'],'price':price})
        position = 0

# close any open position at final close price
if position > 0:
    final_price = df.iloc[-1]['close']
    cash = position * final_price
    trades.append({'type':'EXIT','time':df.iloc[-1]['timestamp'],'price':final_price})
    position = 0

# 6. Report results
total_return = (cash - initial_cash) / initial_cash * 100
wins = sum(
    1 for i in range(1, len(trades), 2)
    if trades[i]['price'] > trades[i-1]['price']
)
num_trades = len(trades) // 2
win_rate = wins / num_trades * 100 if num_trades else 0

print(f"Initial cash: ${initial_cash:,.2f}")
print(f"Final cash:   ${cash:,.2f}")
print(f"Total return: {total_return:.2f}%")
print(f"Trades:       {num_trades}")
print(f"Win rate:     {win_rate:.1f}%")
