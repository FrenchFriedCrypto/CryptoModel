import pandas as pd

def generate_signals(candles_target: pd.DataFrame, candles_anchor: pd.DataFrame) -> pd.DataFrame:
    """
    Strategy: Buy LDO if BTC or ETH pumped >2% exactly 4 hours ago.

    Inputs:
    - candles_target: OHLCV for LDO (1H)
    - candles_anchor: Merged OHLCV with columns 'close_BTC' and 'close_ETH' (1H)

    Output:
    - DataFrame with ['timestamp', 'signal']
    """
    try:
        df = pd.merge(
            candles_target[['timestamp', 'close']],
            candles_anchor[['timestamp', 'close_BTC', 'close_ETH']],
            on='timestamp',
            how='inner'
        )

        df['btc_return_4h_ago'] = df['close_BTC'].pct_change().shift(4)
        df['eth_return_4h_ago'] = df['close_ETH'].pct_change().shift(4)

        signals = []
        for i in range(len(df)):
            btc_pump = df['btc_return_4h_ago'].iloc[i] > 0.02
            eth_pump = df['eth_return_4h_ago'].iloc[i] > 0.02
            if btc_pump or eth_pump:
                signals.append('BUY')
            else:
                signals.append('HOLD')

        df['signal'] = signals
        return df[['timestamp', 'signal']]

    except Exception as e:
        raise RuntimeError(f"Error in generate_signals: {e}")

def get_coin_metadata() -> dict:
    """
    Specifies the target and anchor coins used in this strategy.

    Returns:
    {
        "target": {"symbol": "LDO", "timeframe": "1H"},
        "anchors": [
            {"symbol": "BTC", "timeframe": "1H"},
            {"symbol": "ETH", "timeframe": "1H"}
        ]
    }
    """
    return {
        "target": {
            "symbol": "LDO",
            "timeframe": "1H"
        },
        "anchors": [
            {"symbol": "BTC", "timeframe": "1H"},
            {"symbol": "ETH", "timeframe": "1H"}
        ]
    }