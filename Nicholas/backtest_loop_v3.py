import os
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
def generate_signals(candles_target: pd.DataFrame,
                     candles_anchor: pd.DataFrame,
                     buy_rules,
                     sell_rules) -> pd.DataFrame:
    df = candles_anchor[['timestamp']].copy()
    for a in ANCHORS:
        df[f"close_{a['symbol']}_{a['timeframe']}"] = candles_anchor[f"close_{a['symbol']}_{a['timeframe']}"]

    signals = []
    for i in range(len(df)):
        buy_ok, sell_ok = True, False

        # BUY: all buy_rules must pass
        for r in buy_rules:
            col    = f"close_{r['symbol']}_{r['timeframe']}"
            change = df[col].pct_change().shift(r['lag']).iloc[i]
            if pd.isna(change):
                buy_ok = False
                break
            thresh = r['change_pct'] / 100
            if (r['direction']=="up"   and change <=  thresh) or \
               (r['direction']=="down" and change >=  thresh):
                buy_ok = False
                break

        # SELL: any sell_rule passes
        for r in sell_rules:
            col    = f"close_{r['symbol']}_{r['timeframe']}"
            change = df[col].pct_change().shift(r['lag']).iloc[i]
            if pd.isna(change):
                continue
            thresh = r['change_pct'] / 100
            if (r['direction']=="down" and change <= thresh) or \
               (r['direction']=="up"   and change >= thresh):
                sell_ok = True

        signals.append("BUY" if buy_ok else "SELL" if sell_ok else "HOLD")

    return pd.DataFrame({"timestamp": df['timestamp'], "signal": signals})

def load_candles(symbol: str, timeframe: str) -> pd.DataFrame:
    fn = os.path.join(DATA_DIR, f"{symbol}_{timeframe}.csv")
    df = pd.read_csv(fn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

# === MAIN LOOP ===
symbols = [
    f[:-len(f"_{TIMEFRAME}.csv")]
    for f in os.listdir(DATA_DIR)
    if f.endswith(f"_{TIMEFRAME}.csv")
]

results = []

for sym in symbols:
    # load target + anchor frames once
    df_tgt = load_candles(sym, TIMEFRAME)
    df_anc = pd.DataFrame({'timestamp': df_tgt['timestamp']})
    for a in ANCHORS:
        tmp = load_candles(a['symbol'], a['timeframe'])
        col = f"close_{a['symbol']}_{a['timeframe']}"
        df_anc = df_anc.merge(
            tmp[['timestamp','close']].rename(columns={'close':col}),
            on='timestamp', how='left'
        )

    # sweep change_pct from -10 to +10 in 0.5 steps
    for cp in np.arange(-10, 10.5, 0.5):
        # override buy rules
        temp_buy = [{**r, 'change_pct': cp} for r in BUY_RULES]

        # generate signals & merge
        sigs   = generate_signals(df_tgt, df_anc, temp_buy, SELL_RULES)
        df_run = df_tgt.merge(sigs, on='timestamp', how='left')\
                       .fillna({'signal':'HOLD'})

        # backtest & track equity curve
        initial_cash   = 10_000.0
        cash, position = initial_cash, 0.0
        equity_curve   = []

        for _, row in df_run.iterrows():
            price = row['open']
            if row['signal']=="BUY" and position==0:
                position = cash / price
                cash     = 0.0
            elif row['signal']=="SELL" and position>0:
                cash     = position * price
                position = 0.0

            # at each step, portfolio value = cash + position*price
            equity_curve.append(cash + position * price)

        # final exit if needed
        if position>0:
            final_price = df_run.iloc[-1]['close']
            cash         = position * final_price
            equity_curve[-1] = cash  # overwrite last
            position     = 0.0

        # compute performance metrics
        portfolio = np.array(equity_curve)

        # returns: period-to-period pct change
        ret = np.diff(portfolio) / portfolio[:-1]

        # Sharpe ratio (annualized at 8760 hours)
        sharpe = (ret.mean() / ret.std(ddof=1)) * np.sqrt(8760) if ret.std(ddof=1)>0 else np.nan

        # max drawdown
        running_max = np.maximum.accumulate(portfolio)
        drawdowns   = (portfolio - running_max) / running_max
        max_dd      = drawdowns.min() * 100  # in %

        # basic trade stats
        trades      = []
        cash2, pos2 = initial_cash, 0.0
        for _, row in df_run.iterrows():
            price = row['open']
            if row['signal']=="BUY" and pos2==0:
                pos2 = cash2 / price; cash2 = 0.0; trades.append(('B',price))
            elif row['signal']=="SELL" and pos2>0:
                cash2 = pos2 * price; pos2 = 0.0; trades.append(('S',price))
        if pos2>0:
            final_price = df_run.iloc[-1]['close']
            cash2        = pos2 * final_price
            trades.append(('E', final_price))

        num_trades = len(trades)//2
        wins       = sum(1 for i in range(1,len(trades),2)
                         if trades[i][1] > trades[i-1][1])
        win_rate   = wins / num_trades * 100 if num_trades>0 else 0

        results.append({
            "Symbol":       sym,
            "cp":           cp,
            "Initial cash": initial_cash,
            "Final cash":   cash,
            "Total return": (cash - initial_cash) / initial_cash * 100,
            "Trades":       num_trades,
            "Win rate":     win_rate,
            "Sharpe ratio": sharpe,
            "Max drawdown": max_dd,
            "BTC_cp":       cp,
            "ETH_cp":       cp,
            "SOL_cp":       cp
        })

# save all results
pd.DataFrame(results).to_csv(RESULTS_FILE, index=False)
print(f"✅ Results written to {RESULTS_FILE}")
