import pandas as pd
from strategy_base import generate_signals, get_coin_metadata

# 1. Helper to load a CSV with a 'timestamp' and 'close' column
def load_candles(symbol: str, timeframe: str) -> pd.DataFrame:
    df = pd.read_csv(f"{symbol}_{timeframe}.csv", parse_dates=['timestamp'])
    # ensure columns: timestamp, open, high, low, close, volume...
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
