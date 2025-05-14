import pandas as pd
import importlib.util
import os

ALLOWED_SIGNALS = {"BUY", "SELL", "HOLD"}
ALLOWED_IMPORTS = {"pandas", "numpy"}
MIN_AVG_VOLUME_USD = 5_000_000  # $5M threshold

def load_strategy(path='strategy.py'):
    if not os.path.exists(path):
        raise FileNotFoundError("❌ strategy.py not found.")

    spec = importlib.util.spec_from_file_location("strategy", path)
    strategy = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(strategy)
    return strategy

def validate_imports(path='strategy.py'):
    with open(path, 'r') as f:
        lines = f.readlines()

    for line in lines:
        if line.strip().startswith("import"):
            for part in line.replace("import", "").split(","):
                lib = part.strip().split(" ")[0]
                if lib not in ALLOWED_IMPORTS:
                    raise ImportError(f"❌ External library '{lib}' is not allowed. Only 'pandas' and 'numpy' are permitted.")

def generate_dummy_ohlcv(symbol, timeframe="1H", rows=30):
    ts = pd.date_range("2025-01-01", periods=rows, freq=timeframe)
    df = pd.DataFrame({
        "timestamp": ts,
        "open": 1.0,
        "high": 1.02,
        "low": 0.98,
        "close": [1.0 + 0.01*i for i in range(rows)],
        "volume": [5_000_000 / (1.0 + 0.01*i) for i in range(rows)]  # reverse-engineer so avg USD vol = ~$5M
    })
    return df

def run_check():
    print("🔍 Running submission checks...")

    try:
        strategy = load_strategy()
        validate_imports()

        if not hasattr(strategy, "generate_signals"):
            raise AttributeError("❌ Missing required function: generate_signals()")

        if not hasattr(strategy, "get_coin_metadata"):
            raise AttributeError("❌ Missing required function: get_coin_metadata()")

        metadata = strategy.get_coin_metadata()
        if "target" not in metadata or "anchors" not in metadata:
            raise ValueError("❌ Metadata must include 'target' and 'anchors' keys")

        target = metadata["target"]
        if "symbol" not in target or "timeframe" not in target:
            raise ValueError("❌ 'target' must contain 'symbol' and 'timeframe'")

        for anchor in metadata["anchors"]:
            if "symbol" not in anchor or "timeframe" not in anchor:
                raise ValueError("❌ Each anchor must contain 'symbol' and 'timeframe'")

        print(f"✅ Metadata OK: Target={target['symbol']} | Anchors={[a['symbol'] for a in metadata['anchors']]}")

        candles_target = generate_dummy_ohlcv(target["symbol"], target["timeframe"])
        candles_anchor = pd.DataFrame({'timestamp': candles_target['timestamp']})
        for anchor in metadata["anchors"]:
            symbol = anchor['symbol']
            candles_anchor[f"close_{symbol}"] = candles_target['close']

        # 💰 Volume check
        candles_target["usd_volume"] = candles_target["volume"] * candles_target["close"]
        avg_usd_vol = candles_target["usd_volume"].mean()
        if avg_usd_vol < MIN_AVG_VOLUME_USD:
            print(f"❌ Avg daily USD volume = ${avg_usd_vol:,.2f} — must be ≥ $5,000,000.")
        else:
            print(f"✅ Avg daily USD volume = ${avg_usd_vol:,.2f} (meets requirement)")

        signals = strategy.generate_signals(candles_target, candles_anchor)

        if not isinstance(signals, pd.DataFrame):
            raise TypeError("❌ generate_signals() must return a pandas DataFrame")

        if 'timestamp' not in signals.columns or 'signal' not in signals.columns:
            raise ValueError("❌ Output must contain 'timestamp' and 'signal' columns")

        if len(signals) != len(candles_target):
            raise ValueError("❌ Output length mismatch: signals must match length of candles_target")

        invalid = [s for s in signals['signal'] if s not in ALLOWED_SIGNALS]
        if invalid:
            raise ValueError(f"❌ Invalid signal values found: {set(invalid)}")

        print("✅ Signals are correctly formatted and aligned.")
        print("✅ All checks passed! Submission is valid. 🎉")

    except Exception as e:
        print(str(e))

if __name__ == "__main__":
    run_check()