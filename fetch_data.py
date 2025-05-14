import requests
import pandas as pd
import time

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
start_time_ms = 1735689600000
end_time_ms = 1746748800000

def fetch_ohlcv(symbol, interval, start_time_ms=start_time_ms, end_time_ms=end_time_ms):
    all_candles = []
    limit = 1000
    current_time = start_time_ms

    while current_time < end_time_ms:
        params = {
            "symbol": symbol.upper(),
            "interval": interval,
            "startTime": current_time,
            "endTime": end_time_ms,
            "limit": limit
        }
        response = requests.get(BINANCE_API_URL, params=params)
        data = response.json()

        if not data:
            break

        all_candles.extend(data)
        current_time = data[-1][0] + 1
        time.sleep(0.2)  # avoid rate limits

    df = pd.DataFrame(all_candles, columns=[
        "timestamp", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])

    df = df[["timestamp", "open", "high", "low", "close", "volume"]]
    df["timestamp"] = pd.to_datetime(df["timestamp"], unit="ms")
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)

    return df

def fetch_all(symbols_with_timeframes, start_time, end_time):
    start_ms = int(pd.Timestamp(start_time).timestamp() * 1000)
    end_ms = int(pd.Timestamp(end_time).timestamp() * 1000)
    all_data = {}

    for name, (symbol, tf) in symbols_with_timeframes.items():
        print(f"Fetching {symbol} ({tf})...")
        df = fetch_ohlcv(symbol, tf, start_ms, end_ms)
        all_data[name] = df

    return all_data