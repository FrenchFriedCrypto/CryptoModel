import pandas as pd

def calc_perf_score(inBal, finalBal, mean_return, std_dev_return, high_w, dd):
    """
    Compute a composite performance score based on:
    - % return capped at 45 points     (min required: 15)
    - Sharpe-like ratio capped at 35 pts (min required: 10)
    - Drawdown penalty up to 20 pts      (min required: 5)
    Total score min required: 60 /100
    """
    # 1) Returns score
    perc_return   = ((finalBal - inBal) / inBal) * 100
    returns_score = min(45, max(0, (perc_return / 300) * 45))

    # 2) “Sharpe” score
    sharpe        = (mean_return - 0.0443) / std_dev_return
    sharpe_score  = min(35, max(0, (sharpe / 5) * 35))

    # 3) Drawdown score
    draw_down = (high_w - dd) / high_w * 100
    dd_score  = max(0, (1 - (draw_down / 50)) * 20)

    # Check against minima
    failed = False
    if returns_score < 15:
        print(f"❌ Profitability check failed: returns_score={returns_score:.2f} < 15")
        failed = True
    if sharpe_score < 10:
        print(f"❌ Sharpe ratio check failed: sharpe_score={sharpe_score:.2f} < 10")
        failed = True
    if dd_score < 5:
        print(f"❌ Drawdown check failed: dd_score={dd_score:.2f} < 5")
        failed = True

    # Final composite
    total_score = returns_score + sharpe_score + dd_score
    if total_score < 60:
        print(f"❌ Total score check failed: total_score={total_score:.2f} < 60")
        failed = True

    if failed:
        print("🚫 Performance evaluation FAILED minimum requirements.\n")
    else:
        print("✅ All checks passed!\n")

    return total_score

def main():
    # Single sample scenario
    data = [
        {
            'inBal': 10000, 'finalBal': 20000,
            'mean_return': 0.06, 'std_dev_return': 0.03,
            'high_w': 22000, 'dd': 19000
        },
    ]

    df = pd.DataFrame(data)
    df['perf_score'] = df.apply(
        lambda r: calc_perf_score(
            r['inBal'], r['finalBal'],
            r['mean_return'], r['std_dev_return'],
            r['high_w'],    r['dd']
        ),
        axis=1
    )

    print("Results:")
    print(df.to_string(index=False,
        columns=['inBal','finalBal','mean_return','std_dev_return','high_w','dd','perf_score']
    ))

if __name__ == "__main__":
    main()
