import os
import time
import numpy as np
import pandas as pd

# ========== CONFIGURATION (EDIT THIS SECTION ONLY) ==========
TIMEFRAME    = "1H"           # timeframe suffix on your Data/*.csv files
DATA_DIR     = "../Data"      # folder containing SYMBOL_TIMEFRAME.csv
RESULTS_FILE = "results_comparison.csv"

# Anchor definitions & rules
ANCHORS = [
    {"symbol": "BTC", "timeframe": "1H", "lag": 4},
    {"symbol": "ETH", "timeframe": "1H", "lag": 4},
    {"symbol": "SOL", "timeframe": "1H", "lag": 4},
    {"symbol": "ETH", "timeframe": "4H", "lag": 0},
]

# Base BUY rules (we’ll override change_pct in the loop)
BUY_RULES = [
    {"symbol": "BTC", "timeframe": "1H", "lag": 4, "change_pct": -10.0, "direction": "down"},
    {"symbol": "ETH", "timeframe": "1H", "lag": 4, "change_pct": -10.0, "direction": "down"},
    {"symbol": "SOL", "timeframe": "1H", "lag": 4, "change_pct": -10.0, "direction": "down"},
]

# SELL rules remain fixed
SELL_RULES = [
    {"symbol": "ETH", "timeframe": "1H", "lag": 0, "change_pct": -2.0, "direction": "down"},
]


# ========== STRATEGY ENGINE (DO NOT EDIT BELOW) ==========

def load_candles(symbol: str, timeframe: str) -> pd.DataFrame:
    """Load OHLC CSV and parse timestamp."""
    fn = os.path.join(DATA_DIR, f"{symbol}_{timeframe}.csv")
    df = pd.read_csv(fn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

def generate_signals(df_anc: pd.DataFrame,
                     buy_rules: list,
                     sell_rules: list,
                     pct_dict: dict) -> pd.DataFrame:
    """Vector-indexed signal generator using precomputed pct changes."""
    ts = df_anc['timestamp'].to_numpy()
    n  = len(df_anc)
    signals = []

    for i in range(n):
        buy_ok, sell_ok = True, False

        # BUY: all buy_rules must pass
        for r in buy_rules:
            key    = (r['symbol'], r['timeframe'], r['lag'])
            change = pct_dict[key][i]
            if pd.isna(change):
                buy_ok = False
                break
            thresh = r['change_pct'] / 100
            if ((r['direction']=="up"   and change <=  thresh) or
                (r['direction']=="down" and change >=  thresh)):
                buy_ok = False
                break

        # SELL: any sell_rule passes
        for r in sell_rules:
            key    = (r['symbol'], r['timeframe'], r['lag'])
            change = pct_dict[key][i]
            if pd.isna(change):
                continue
            thresh = r['change_pct'] / 100
            if ((r['direction']=="down" and change <= thresh) or
                (r['direction']=="up"   and change >= thresh)):
                sell_ok = True

        if buy_ok:
            signals.append("BUY")
        elif sell_ok:
            signals.append("SELL")
        else:
            signals.append("HOLD")

    return pd.DataFrame({"timestamp": ts, "signal": signals})


# === MAIN LOOP ===

start_time = time.time()

# find all target symbols in DATA_DIR
# symbols = [
#     f[:-len(f"_{TIMEFRAME}.csv")]
#     for f in os.listdir(DATA_DIR)
#     if f.endswith(f"_{TIMEFRAME}.csv")
# ]

symbols = ['AAVE']

results = []

for sym in symbols:
    # 1) load target
    df_tgt = load_candles(sym, TIMEFRAME)

    # 2) build anchor DataFrame
    df_anc = pd.DataFrame({'timestamp': df_tgt['timestamp']})
    for a in ANCHORS:
        tmp = load_candles(a['symbol'], a['timeframe'])
        col = f"close_{a['symbol']}_{a['timeframe']}"
        df_anc = df_anc.merge(
            tmp[['timestamp','close']].rename(columns={'close':col}),
            on='timestamp', how='left'
        )

    # 3) PRECOMPUTE all pct-change + shift for every rule combo
    pct_dict = {}
    for r in BUY_RULES + SELL_RULES:
        key = (r['symbol'], r['timeframe'], r['lag'])
        col = f"close_{r['symbol']}_{r['timeframe']}"
        pct_dict[key] = df_anc[col].pct_change().shift(r['lag']).to_numpy()

    # 4) parameter sweep & backtest
    for cp in np.arange(-10, 10.5, 0.5):
        temp_buy = [{**r, 'change_pct': cp} for r in BUY_RULES]
        sigs = generate_signals(df_anc, temp_buy, SELL_RULES, pct_dict)
        df_run = df_tgt.merge(sigs, on='timestamp', how='left') \
                       .fillna({'signal':'HOLD'})

        # backtest & equity curve
        initial_cash = 10_000.0
        cash, position = initial_cash, 0.0
        equity_curve = []
        for _, row in df_run.iterrows():
            price = row['open']
            if row['signal']=="BUY" and position==0:
                position = cash / price; cash = 0.0
            elif row['signal']=="SELL" and position>0:
                cash = position * price; position = 0.0
            equity_curve.append(cash + position * price)

        # final exit
        if position>0:
            final_price = df_run.iloc[-1]['close']
            cash = position * final_price
            equity_curve[-1] = cash
            position = 0.0

        # metrics
        portfolio = np.array(equity_curve)
        ret       = np.diff(portfolio) / portfolio[:-1]
        sharpe    = (ret.mean() / ret.std(ddof=1)) * np.sqrt(8760) if ret.std(ddof=1)>0 else np.nan
        running_max = np.maximum.accumulate(portfolio)
        drawdowns   = (portfolio - running_max) / running_max
        max_dd      = drawdowns.min() * 100

        # trade stats
        trades = []
        cash2, pos2 = initial_cash, 0.0
        for _, row in df_run.iterrows():
            price = row['open']
            if row['signal']=="BUY" and pos2==0:
                pos2 = cash2 / price; cash2 = 0.0; trades.append(('B', price))
            elif row['signal']=="SELL" and pos2>0:
                cash2 = pos2 * price; pos2 = 0.0; trades.append(('S', price))
        if pos2>0:
            final_price = df_run.iloc[-1]['close']
            cash2 = pos2 * final_price
            trades.append(('E', final_price))

        num_trades = len(trades)//2
        wins       = sum(1 for i in range(1, len(trades), 2)
                         if trades[i][1] > trades[i-1][1])
        win_rate   = wins / num_trades * 100 if num_trades>0 else 0

        results.append({
            "Symbol":        sym,
            "cp":            cp,
            "Initial cash":  initial_cash,
            "Final cash":    cash,
            "Total return":  (cash - initial_cash) / initial_cash * 100,
            "Trades":        num_trades,
            "Win rate":      win_rate,
            "Sharpe ratio":  sharpe,
            "Max drawdown":  max_dd,
            "BTC_cp":        cp,
            "ETH_cp":        cp,
            "SOL_cp":        cp
        })

# 5) save all results
pd.DataFrame(results).to_csv(RESULTS_FILE, index=False)
print(f"✅ Results written to {RESULTS_FILE}")

end_time = time.time()
print(f"⏱ Total processing time: {end_time - start_time:.2f} seconds")

# Not using Iloc
# ⏱ Total processing time: 20.36 seconds

# using Iloc
# ⏱ Total processing time: 171.85 seconds



# ⏱ Total processing time: 2196.06 seconds