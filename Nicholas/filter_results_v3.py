#!/usr/bin/env python3
import sys
import pandas as pd

def analyze_positive_filters(filename,
                             cash_threshold,
                             sharpe_threshold,
                             drawdown_threshold):
    # load data
    df = pd.read_csv(filename)

    # 1) positive Entry_cp
    pos = df[df['Entry_cp'] > 0]
    total_pos = len(pos)
    if total_pos == 0:
        print("No rows with positive Entry_cp found.")
        return

    # 2) Final cash > cash_threshold
    cash_pass = pos[pos['Final cash'] > cash_threshold]
    n_cash    = len(cash_pass)
    pct_cash  = n_cash / total_pos * 100
    print(f"Final cash > {cash_threshold}: {n_cash} out of {total_pos}, or {pct_cash:.2f}%")

    if n_cash == 0:
        print("No rows passed the Final cash filter, skipping further analysis.")
        return

    # 3) Sharpe ratio > sharpe_threshold
    sharpe_pass = cash_pass[cash_pass['Sharpe ratio'] > sharpe_threshold]
    n_sharpe    = len(sharpe_pass)
    pct_sharpe  = n_sharpe / n_cash * 100
    print(f"Sharpe ratio > {sharpe_threshold}: {n_sharpe} out of {n_cash}, or {pct_sharpe:.2f}%")

    if n_sharpe == 0:
        print("No rows passed the Sharpe ratio filter, skipping drawdown analysis.")
        return

    # 4) Max drawdown > drawdown_threshold (i.e. less severe drawdowns)
    dd_pass    = sharpe_pass[sharpe_pass['Max drawdown'] > drawdown_threshold]
    n_dd       = len(dd_pass)
    pct_dd     = n_dd / n_sharpe * 100
    print(f"Max drawdown > {drawdown_threshold}: {n_dd} out of {n_sharpe}, or {pct_dd:.2f}%")

    if n_dd == 0:
        print("No rows passed the drawdown filter.")
        return

    # 5) Print unique symbols in the final group
    unique_symbols = dd_pass['Symbol'].unique()
    print("\nUnique symbols in the final group:")
    for sym in unique_symbols:
        print(sym)

if __name__ == '__main__':
    INPUT_CSV         = "results_comparison_SOL_SL.csv"
    CASH_THRESHOLD     = 14000.0
    SHARPE_THRESHOLD   = 2
    DRAWDOWN_THRESHOLD = -10.0

    analyze_positive_filters(
        INPUT_CSV,
        CASH_THRESHOLD,
        SHARPE_THRESHOLD,
        DRAWDOWN_THRESHOLD
    )
