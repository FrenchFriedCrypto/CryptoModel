#!/usr/bin/env python3
import sys
import pandas as pd

def summarize_by_entry_cp(filename, threshold):
    # load data
    df = pd.read_csv(filename)

    # split into negative and positive Entry_cp (zeros are ignored)
    groups = {
        'Negative Entry_cp': df[df['Entry_cp'] < 0],
        'Positive Entry_cp': df[df['Entry_cp'] > 0]
    }

    # for each group, count how many have Final cash > threshold
    for name, group in groups.items():
        total = len(group)
        if total == 0:
            print(f"{name}: no rows in this category")
            continue
        count = (group['Final cash'] > threshold).sum()
        print(f"{name}: {count} out of {total}, or {count/total*100:.2f}%")

if __name__ == '__main__':
    INPUT_CSV = "results_comparison_SOL_SL.csv"
    THRESHOLD = 12000.0
    summarize_by_entry_cp(INPUT_CSV, THRESHOLD)

