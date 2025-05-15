import os
import pandas as pd
import numpy as np  # for drawdowns and Sharpe

# ========== CONFIGURATION (EDIT THIS SECTION ONLY) ==========

# 🚀 TARGET COIN to trade (this is the coin you'll generate BUY/SELL/HOLD for)
TARGET_COIN = "LDO"       # Example: "LDO", "BONK", "RAY"
TIMEFRAME   = "1H"        # Timeframe of your strategy: "1H", "4H", "1D"

# 🧭 ANCHOR COINS used to derive signals
ANCHORS = [
    {"symbol": "BTC", "timeframe": "1H", "lag": 4},
    {"symbol": "ETH", "timeframe": "1H", "lag": 4},
    {"symbol": "ETH", "timeframe": "4H", "lag": 0}
]

# ✅ BUY RULES
BUY_RULES = [
    {"symbol": "BTC", "timeframe": "1H", "lag": 4, "change_pct": 3.0, "direction": "up"},
    {"symbol": "ETH", "timeframe": "1H", "lag": 4, "change_pct": 5.0, "direction": "up"}
]

# ❌ SELL RULES
SELL_RULES = [
    {"symbol": "ETH", "timeframe": "4H", "lag": 0, "change_pct": -3.0, "direction": "down"}
]

# ========== STRATEGY ENGINE (DO NOT EDIT BELOW) ==========

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    df = candles_target[['timestamp']].copy()
    for a in ANCHORS:
        col = f"close_{a['symbol']}_{a['timeframe']}"
        df[col] = candles_anchor[col].values

    signals = []
    for i in range(len(df)):
        buy_pass = True
        sell_pass = False

        # BUY rules
        for r in BUY_RULES:
            col = f"close_{r['symbol']}_{r['timeframe']}"
            change = df[col].pct_change().shift(r['lag']).iloc[i]
            if pd.isna(change) or (r['direction']=='up'   and change<= r['change_pct']/100) \
                              or (r['direction']=='down' and change>= r['change_pct']/100):
                buy_pass = False
                break

        # SELL rules
        for r in SELL_RULES:
            col = f"close_{r['symbol']}_{r['timeframe']}"
            change = df[col].pct_change().shift(r['lag']).iloc[i]
            if not pd.isna(change):
                if (r['direction']=='down' and change<= r['change_pct']/100) \
                or  (r['direction']=='up'   and change>= r['change_pct']/100):
                    sell_pass = True

        signals.append("BUY" if buy_pass else "SELL" if sell_pass else "HOLD")

    return pd.DataFrame({'timestamp': df['timestamp'], 'signal': signals})


def get_coin_metadata() -> dict:
    return {
        "target": {"symbol": TARGET_COIN, "timeframe": TIMEFRAME},
        "anchors": [{"symbol": a["symbol"], "timeframe": a["timeframe"]} for a in ANCHORS]
    }


def load_candles(symbol: str, timeframe: str) -> pd.DataFrame:
    df = pd.read_csv(f"Data/{symbol}_{timeframe}.csv")
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df


# ─── Load data ──────────────────────────────────────────────────────────────────
meta       = get_coin_metadata()
df_target  = load_candles(meta['target']['symbol'], meta['target']['timeframe'])

df_anchor = pd.DataFrame({'timestamp': df_target['timestamp']})
for a in meta['anchors']:
    df = load_candles(a['symbol'], a['timeframe'])
    col = f"close_{a['symbol']}_{a['timeframe']}"
    df_anchor = df_anchor.merge(
        df[['timestamp', 'close']].rename(columns={'close': col}),
        on='timestamp', how='left'
    )

signals = generate_signals(df_target, df_anchor)
df = df_target.merge(signals, on='timestamp', how='left').fillna({'signal':'HOLD'})


# ─── Backtest ─────────────────────────────────────────────────────────────────
initial_cash  = 10_000.0
cash          = initial_cash
position      = 0.0
equity_curve  = []
trades        = []

for _, row in df.iterrows():
    sig   = row['signal']
    price = row['open']

    if sig == 'BUY' and position == 0:
        position = cash / price
        cash     = 0.0
        trades.append({'type':'BUY',  'time':row['timestamp'], 'price':price})

    elif sig == 'SELL' and position > 0:
        cash     = position * price
        trades.append({'type':'SELL', 'time':row['timestamp'], 'price':price})
        position = 0.0

    # record equity each bar
    equity = cash + position * price
    equity_curve.append(equity)

# exit any open at the end
if position > 0:
    final_price = df.iloc[-1]['close']
    cash       = position * final_price
    trades.append({'type':'EXIT', 'time':df.iloc[-1]['timestamp'], 'price':final_price})
    position   = 0.0
    equity_curve[-1] = cash  # overwrite last bar to reflect exit


# ─── Performance ───────────────────────────────────────────────────────────────
# Total return & basic stats
total_return = (cash - initial_cash) / initial_cash * 100
num_trades   = len(trades) // 2
wins         = sum(1 for i in range(1, len(trades), 2)
                   if trades[i]['price'] > trades[i-1]['price'])
win_rate     = wins / num_trades * 100 if num_trades else 0

# Max drawdown
eq       = np.array(equity_curve)
running_max   = np.maximum.accumulate(eq)
drawdowns     = (running_max - eq) / running_max
max_drawdown  = np.max(drawdowns) * 100

# Sharpe ratio (assumes zero risk-free)
# compute period returns
rets = np.diff(eq) / eq[:-1]
# annualize factor based on timeframe
if TIMEFRAME.endswith('H'):
    hrs = int(TIMEFRAME[:-1])
    periods_per_year = 365 * 24 / hrs
elif TIMEFRAME.endswith('D'):
    days = int(TIMEFRAME[:-1])
    periods_per_year = 365 / days
else:
    periods_per_year = 252  # fallback

sharpe_ratio = (np.mean(rets) / np.std(rets, ddof=1)) * np.sqrt(periods_per_year) \
    if rets.std(ddof=1) != 0 else np.nan

# ─── Print report ──────────────────────────────────────────────────────────────
print(f"Initial cash:    ${initial_cash:,.2f}")
print(f"Final cash:      ${cash:,.2f}")
print(f"Total return:    {total_return:.2f}%")
print(f"Trades:          {num_trades}")
print(f"Win rate:        {win_rate:.1f}%")
print(f"Max drawdown:    {max_drawdown:.2f}%")
print(f"Sharpe ratio:    {sharpe_ratio:.2f}")
