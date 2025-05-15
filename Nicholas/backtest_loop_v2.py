import os
import pandas as pd

# ========== CONFIGURATION (EDIT THIS SECTION ONLY) ==========

TIMEFRAME       = "1H"         # timeframe suffix on your Data/*.csv files
DATA_DIR        = "../Data"       # folder containing SYMBOL_TIMEFRAME.csv
RESULTS_FILE    = "results_comparison.csv"

# Anchor definitions & rules (unchanged)
ANCHORS = [
    {"symbol": "BTC", "timeframe": "1H", "lag": 4},
    {"symbol": "ETH", "timeframe": "1H", "lag": 4},
    {"symbol": "SOL", "timeframe": "1H", "lag": 4},
    {"symbol": "ETH", "timeframe": "1H", "lag": 0},
]

BUY_RULES = [
    {"symbol": "BTC", "timeframe": "1H", "lag": 4, "change_pct": 3.0, "direction": "up"},
    {"symbol": "SOL", "timeframe": "1H", "lag": 4, "change_pct": 3.0, "direction": "up"},
    {"symbol": "ETH", "timeframe": "1H", "lag": 4, "change_pct": 3.5, "direction": "up"}
]

SELL_RULES = [
    {"symbol": "ETH", "timeframe": "4H", "lag": 0, "change_pct": -3.0, "direction": "down"},
]

# ========== STRATEGY ENGINE (DO NOT EDIT BELOW) ==========

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    df = candles_anchor[['timestamp']].copy()
    # merge all anchor closes
    for a in ANCHORS:
        col = f"close_{a['symbol']}_{a['timeframe']}"
        if col not in candles_anchor:
            raise ValueError(f"Missing column: {col}")
        df[col] = candles_anchor[col]

    signals = []
    for i in range(len(df)):
        buy_ok  = True
        sell_ok = False

        # BUY logic: all BUY_RULES must pass
        for r in BUY_RULES:
            col = f"close_{r['symbol']}_{r['timeframe']}"
            change = df[col].pct_change().shift(r['lag']).iloc[i]
            if pd.isna(change):
                buy_ok = False; break
            if (r['direction']=="up"   and change <=  r['change_pct']/100) or \
               (r['direction']=="down" and change >=  r['change_pct']/100):
                buy_ok = False; break

        # SELL logic: any SELL_RULES pass
        for r in SELL_RULES:
            col = f"close_{r['symbol']}_{r['timeframe']}"
            change = df[col].pct_change().shift(r['lag']).iloc[i]
            if pd.isna(change):
                continue
            if (r['direction']=="down" and change <= r['change_pct']/100) or \
               (r['direction']=="up"   and change >= r['change_pct']/100):
                sell_ok = True

        signals.append("BUY" if buy_ok else "SELL" if sell_ok else "HOLD")

    return pd.DataFrame({
        "timestamp": df['timestamp'],
        "signal":   signals
    })

def load_candles(symbol: str, timeframe: str) -> pd.DataFrame:
    fn = os.path.join(DATA_DIR, f"{symbol}_{timeframe}.csv")
    df = pd.read_csv(fn)
    df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms')
    return df

# === MAIN LOOP ===

# find all target symbols in Data/
symbols = []
for fname in os.listdir(DATA_DIR):
    if fname.endswith(f"_{TIMEFRAME}.csv"):
        symbols.append(fname[:-len(f"_"+TIMEFRAME+".csv")])

results = []
for sym in symbols:
    # load target + anchor frames
    df_tgt = load_candles(sym, TIMEFRAME)
    df_anc = pd.DataFrame({'timestamp': df_tgt['timestamp']})
    for a in ANCHORS:
        df_temp = load_candles(a['symbol'], a['timeframe'])
        col = f"close_{a['symbol']}_{a['timeframe']}"
        df_anc = df_anc.merge(
            df_temp[['timestamp','close']].rename(columns={'close':col}),
            on='timestamp', how='left'
        )

    # generate signals & merge
    sigs = generate_signals(df_tgt, df_anc)
    df_run = df_tgt.merge(sigs, on='timestamp', how='left').fillna({'signal':'HOLD'})

    # backtest
    initial_cash = 10_000.0
    cash = initial_cash
    position = 0.0
    trades = []
    for _, row in df_run.iterrows():
        price = row['open']
        if row['signal']=="BUY" and position==0:
            position = cash/price
            cash = 0
            trades.append(('BUY', price))
        elif row['signal']=="SELL" and position>0:
            cash = position*price
            trades.append(('SELL',price))
            position = 0
    # exit final
    if position>0:
        final_price = df_run.iloc[-1]['close']
        cash = position*final_price
        trades.append(('EXIT',final_price))

    # metrics
    total_return = (cash-initial_cash)/initial_cash*100
    num_trades  = len(trades)//2
    wins = sum(1 for i in range(1,len(trades),2)
               if trades[i][1] > trades[i-1][1])
    win_rate = wins/num_trades*100 if num_trades>0 else 0

    results.append({
        "Initial cash":  initial_cash,
        "Final cash":    cash,
        "Total return":  total_return,
        "Trades":        num_trades,
        "Win rate":      win_rate
    })

# save all symbols’ results
pd.DataFrame(results).to_csv(RESULTS_FILE, index=False)
print(f"✅ Results written to {RESULTS_FILE}")
