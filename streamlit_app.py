import streamlit as st
import json
from textwrap import indent

st.set_page_config(page_title="Lunor AI: PairWise Alpha Strategy Generator", layout="wide")

# === Display Lunor Logo ===
st.image("lunor-full.png", width=160)

# ========== HEADER & CONTEXT ==========
st.title("🔧 Strategy Generator for PairWise Alpha Challenge")
st.markdown("""
Welcome to the official strategy builder for **PairWise Alpha**, Lunor's signature quest where traders and data scientists compete to build the most profitable lag-based crypto strategies.

PairWise Alpha asks:
> Can you build a deterministic strategy that exploits lagged correlations between coins like BONK and SOL?

🧠 Strategies are evaluated on:
- Profitability
- Sharpe Ratio
- Max Drawdown

💬 Need Help? Join discussions, get help, and connect with the Lunor AI community:
👉 [Join our Discord](https://discord.gg/6NrZmpPpTY)

📘 [Read the full challenge brief](https://app.lunor.quest/challenge/1000036)

---
""")

# --- Inputs for Target Coin ---
st.subheader("🎯 Target Coin")
target_symbol = st.text_input("Target Coin Symbol", value="LDO")
target_timeframe = st.selectbox("Target Timeframe", ["1H", "4H", "1D"], index=0)

# --- Anchors ---
st.subheader("🧭 Anchor Coins")
anchors = []
num_anchors = st.number_input("How many anchor coins?", min_value=1, max_value=8, value=2)
for i in range(num_anchors):
    with st.expander(f"Anchor #{i+1}"):
        symbol = st.text_input(f"Anchor {i+1} Symbol", key=f"a_sym_{i}", value="BTC" if i == 0 else "ETH")
        tf = st.selectbox(f"Anchor {i+1} Timeframe", ["1H", "4H", "1D"], key=f"a_tf_{i}")
        lag = st.number_input(f"Lag (in candles) for Anchor {i+1}", min_value=0, max_value=48, value=4, key=f"a_lag_{i}")
        anchors.append({"symbol": symbol.upper(), "timeframe": tf, "lag": lag})

# --- Buy Rules ---
st.subheader("📈 BUY Rules")
buy_rules = []
num_buy = st.number_input("Number of BUY rules (all must be true)", min_value=1, max_value=8, value=2)
for i in range(num_buy):
    with st.expander(f"BUY Rule #{i+1}"):
        symbol = st.text_input(f"BUY Rule {i+1} Symbol", key=f"b_sym_{i}", value="BTC")
        tf = st.selectbox(f"BUY Rule {i+1} Timeframe", ["1H", "4H", "1D"], key=f"b_tf_{i}")
        lag = st.number_input(f"Lag (candles)", value=4, key=f"b_lag_{i}")
        pct = st.number_input(f"% Change Required", value=2.0, key=f"b_pct_{i}")
        direction = st.selectbox("Direction", ["up", "down"], key=f"b_dir_{i}")
        buy_rules.append({"symbol": symbol.upper(), "timeframe": tf, "lag": lag, "change_pct": pct, "direction": direction})

# --- Sell Rules ---
st.subheader("📉 SELL Rules")
sell_rules = []
num_sell = st.number_input("Number of SELL rules (any can trigger SELL)", min_value=1, max_value=8, value=1)
for i in range(num_sell):
    with st.expander(f"SELL Rule #{i+1}"):
        symbol = st.text_input(f"SELL Rule {i+1} Symbol", key=f"s_sym_{i}", value="ETH")
        tf = st.selectbox(f"SELL Rule {i+1} Timeframe", ["1H", "4H", "1D"], key=f"s_tf_{i}")
        lag = st.number_input(f"Lag (candles)", value=0, key=f"s_lag_{i}")
        pct = st.number_input(f"% Change Required", value=-3.0, key=f"s_pct_{i}")
        direction = st.selectbox("Direction", ["up", "down"], key=f"s_dir_{i}")
        sell_rules.append({"symbol": symbol.upper(), "timeframe": tf, "lag": lag, "change_pct": pct, "direction": direction})

# --- Generate Python ---
def format_list(name, items):
    lines = json.dumps(items, indent=4).replace('true', 'True').replace('false', 'False')
    return f"{name} = {lines}\n"

if st.button("🚀 Generate strategy.py"):
    code = f"""import pandas as pd

# === CONFIGURATION ===
TARGET_COIN = \"{target_symbol.upper()}\"
TIMEFRAME = \"{target_timeframe}\"

ANCHORS = {json.dumps(anchors, indent=4).replace('true', 'True')}

BUY_RULES = {json.dumps(buy_rules, indent=4).replace('true', 'True')}

SELL_RULES = {json.dumps(sell_rules, indent=4).replace('true', 'True')}

# === STRATEGY ENGINE ===
def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    try:
        df = candles_target[['timestamp']].copy()
        for anchor in ANCHORS:
            col = f\"close_{{anchor['symbol']}}_{{anchor['timeframe']}}\"
            if col not in candles_anchor.columns:
                raise ValueError(f\"Missing column: {{col}}\")
            df[col] = candles_anchor[col].values

        signals = []
        for i in range(len(df)):
            buy_pass = True
            sell_pass = False

            for rule in BUY_RULES:
                col = f\"close_{{rule['symbol']}}_{{rule['timeframe']}}\"
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
                col = f\"close_{{rule['symbol']}}_{{rule['timeframe']}}\"
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
        raise RuntimeError(f\"Strategy failed: {{e}}\")

def get_coin_metadata() -> dict:
    return {{
        "target": {{"symbol": TARGET_COIN, "timeframe": TIMEFRAME}},
        "anchors": [{{"symbol": a["symbol"], "timeframe": a["timeframe"]}} for a in ANCHORS]
    }}
"""
    st.code(code, language="python")
    st.download_button("📥 Download strategy.py", data=code, file_name="strategy.py", mime="text/x-python")
