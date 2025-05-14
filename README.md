# 🧠 PairWise Alpha Starter Kit

Welcome to the official starter repo for the [**Lunor Quest: PairWise Alpha**](https://app.lunor.quest/challenge/1000036) challenge.

Your mission is to create a **deterministic trading strategy** that identifies coins correlated with **BTC, ETH, or SOL** — even with a time lag — and executes trades on the **Target Coin** based on their behavior.

---

## 🚀 Getting Started

1. **Fork this repository**
2. Open and edit `strategy.py` with your logic
3. Run `submission_check.py` to validate your submission
4. Submit only your final `strategy.py` file to the Lunor Quest platform

---

## 🗂️ Files in This Repo

| File                  | Description |
|-----------------------|-------------|
| `strategy.py` (Submit ONLY this file) | Starter template for your strategy |
| `submission_check.py`  | Local validator to ensure your code meets all requirements |
| `fetch_data.py`  | Helper script to fetch data from Binance |


---

## 🧪 Your Strategy Must Implement

### `generate_signals(candles_target, candles_anchor)`

Takes OHLCV data (pandas DataFrames) for:
- **candles_target**: the coin you're trading
- **candles_anchor**: merged OHLCV for anchor coins like BTC, ETH, SOL

Returns a DataFrame with:
```python
[
  {"timestamp": ..., "signal": "BUY" or "SELL" or "HOLD"},
  ...
]
```

---

### `get_coin_metadata()`

Returns metadata about which coins and timeframes you're using:
for (e.g.)

```python
{
  "target": {
    "symbol": "LDO",
    "timeframe": "1H"
  },
  "anchors": [
    {"symbol": "BTC", "timeframe": "1H"},
    {"symbol": "ETH", "timeframe": "1H"}
  ]
}
```

> ⚠️ **Important**: Any anchor coin referenced in your signal logic (e.g., `close_BTC`) **must be listed** in this metadata.  
> Otherwise your submission will **fail** during validation and be disqualified.

---

## 📊 OHLCV Data Expectations

### 🎯 `candles_target` DataFrame

| Column     | Description           |
|------------|-----------------------|
| `timestamp`| ISO format timestamp  |
| `open`     | Open price            |
| `high`     | High price            |
| `low`      | Low price             |
| `close`    | Close price           |
| `volume`   | Trading volume (token units) |

---

### 🛰️ `candles_anchor` DataFrame

Includes full OHLCV data for each anchor coin, prefixed by symbol:

| Column Format        | Description                         |
|----------------------|-------------------------------------|
| `timestamp`          | Shared with target coin             |
| `open_<SYMBOL>`      | Open price of anchor coin           |
| `high_<SYMBOL>`      | High price of anchor coin           |
| `low_<SYMBOL>`       | Low price of anchor coin            |
| `close_<SYMBOL>`     | Close price of anchor coin          |
| `volume_<SYMBOL>`    | Trading volume of anchor coin       |

> Example:
```python
candles_anchor.columns
# ['timestamp', 'open_BTC', 'high_BTC', 'low_BTC', 'close_BTC', 'volume_BTC',
#  'open_ETH', 'high_ETH', 'low_ETH', 'close_ETH', 'volume_ETH']
```

---

## ⚠️ Rules & Restrictions

| Rule | Description |
|------|-------------|
| ✅ **Target Coin** | Must be Binance-listed and have **average daily USD volume ≥ $5M** from Jan 1 2025, 00:00:00 UTC – May 9 2025, 00:00:00 UTC  |
| ✅ **Anchor Coins** | Must be **BTC**, **ETH**, or **SOL** |
| ✅ **Timeframes** | Only `1H`, `4H`, and `1D` are allowed |
| ❌ **No External Data** | Only use OHLCV from Binance |
| ❌ **No External Libraries** | Only `pandas` and `numpy` allowed |
| ✅ **Deterministic** | Output must be 100% reproducible from candles (no randomness or future leak logic) |

---

## ✅ Run the Submission Check

```bash
python submission_check.py
```

This script checks:
- [x] Required functions exist
- [x] Signal format is correct
- [x] Metadata matches strategy logic
- [x] Only allowed libraries are imported
- [x] Signal length matches candles
- [x] Signal values are valid (`BUY`, `SELL`, `HOLD`)
- [x] Avg daily USD volume ≥ $5M (calculated from dummy OHLCV)

---

## 🏁 Final Submission

Once you're validated:
- Submit just your `strategy.py` file on the Lunor Quest portal.

You’ll be evaluated on:
- 📈 Profitability
- 📊 Sharpe Ratio
- 📉 Max Drawdown

## Cutoff Policy 

<img src="https://github.com/user-attachments/assets/07c6d25e-7c2e-425d-ab60-725888ee696e" width="350">

Good luck finding your pairwise alpha! 🧠🚀



### For more details, check out the challenge page on [Lunor Quest](https://app.lunor.quest)
