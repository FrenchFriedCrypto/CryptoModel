import requests
import csv
import time
from datetime import datetime, timezone, timedelta
import ssl
import os
import pandas as pd

def process_data_for_symbol(host_url, symbol, interval, output_folder):
    headers = {'Accept': 'application/json', 'Content-Type': 'application/json'}

    # Set end_date as timezone-aware datetime object (UTC)
    end_date = datetime.now(timezone.utc)

    # Helper to convert datetime to ms timestamp
    def to_ms(dt: datetime) -> int:
        return int(dt.timestamp() * 1000)

    # Strip USDT suffix for the base name
    if symbol.endswith("USDT"):
        base_symbol = symbol[:-4]
    else:
        base_symbol = symbol

    # Build the filename with uppercase interval
    filename = f"{base_symbol}_{interval.upper()}.csv"
    csv_file_path = os.path.join(output_folder, filename)

    # Determine where to start from
    if os.path.exists(csv_file_path) and os.path.getsize(csv_file_path) > 0:
        existing_data = pd.read_csv(csv_file_path, usecols=['Open time'])
        last_ts = int(existing_data['Open time'].max())
        current_date = datetime.fromtimestamp(last_ts / 1000, tz=timezone.utc)
        print(f"Resuming {symbol} from {current_date.isoformat()} (ts={last_ts})")
    else:
        # start from 1 Jan 2025 UTC
        current_date = datetime.strptime("01/01/25", "%d/%m/%y").replace(tzinfo=timezone.utc)
        last_ts = to_ms(current_date)
        print(f"Starting {symbol} from {current_date.isoformat()} (ts={last_ts})")

    while current_date < end_date:
        # compute next_date by days_gap or minutes for '1m'
        if interval == '1m':
            next_date = current_date + timedelta(minutes=days_gap)
        else:
            next_date = current_date + timedelta(days=days_gap)

        start_time = to_ms(current_date)
        end_time = to_ms(next_date)
        params = (
            f"symbol={symbol}&interval={interval}"
            f"&startTime={start_time}&endTime={end_time}&limit=1000"
        )

        # fetch with up to 2 retries
        max_retries, retry_count, success = 2, 0, False
        while retry_count < max_retries and not success:
            try:
                resp = requests.get(host_url, params=params, headers=headers)
                time.sleep(0.05)
                if resp.status_code == 429:
                    raise Exception("Rate limit exceeded")
                data = resp.json()
                # drop the last partial bar if present
                if len(data) > 1:
                    data = data[:-1]
                else:
                    print(f"  [!] Not enough data for {symbol}: {start_time}–{end_time}")
                    break
                success = True
            except (requests.exceptions.SSLError, ssl.SSLError) as e:
                retry_count += 1
                print(f"SSL error: {e} (retry {retry_count}/{max_retries})")
                time.sleep(1)
            except Exception as e:
                retry_count += 1
                print(f"Error fetching {symbol}: {e} (retry {retry_count}/{max_retries})")
                time.sleep(1)

        if not success:
            print(f"  [!] Skipping period {current_date}–{next_date}")
            current_date = next_date
            continue

        # If we got any bars back, keep only the first 6 fields
        if data:
            # write headers if new file
            if not os.path.exists(csv_file_path) or os.path.getsize(csv_file_path) == 0:
                with open(csv_file_path, 'w', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerow([
                        "Open time", "Open", "High", "Low", "Close", "Volume"
                    ])

            final_arr = []
            for row in data:
                ts_open = float(row[0])  # ms
                if ts_open > last_ts:
                    # take only first 6 elements: Open time, Open, High, Low, Close, Volume
                    final_arr.append(row[:6])

            if final_arr:
                with open(csv_file_path, 'a', newline='') as f:
                    writer = csv.writer(f)
                    writer.writerows(final_arr)
                last_ts = float(final_arr[-1][0])
            else:
                print(f"  [!] No new rows for {symbol} in this batch")
        else:
            print(f"  [!] Empty data for {symbol} between {current_date} and {next_date}")

        current_date = next_date


def read_and_clean_csv(filepath):
    df = pd.read_csv(filepath, index_col=False)
    # drop any Unnamed columns
    df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
    return df


def process_csv_files(folder_path):
    for fn in os.listdir(folder_path):
        if fn.endswith(".csv"):
            path = os.path.join(folder_path, fn)
            print(f"Cleaning {path}")
            df = read_and_clean_csv(path)
            df.to_csv(path, index=False)


# ──── Main ─────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    host = "https://api.binance.com"
    # host = "https://api1.binance.com"
    # host = "https://api2.binance.com"
    # host = "https://api3.binance.com"
    # host = "https://api4.binance.com"
    prefix = "/api/v3/klines"
    host_url = host + prefix

    intervals = ['1d']  # you can add other intervals here
    days_gap_mapping = {
        '1m': 1000, '3m': 2,   '5m': 3,   '15m': 10,
        '30m': 20,  '1h': 40,  '2h': 80,  '4h': 160,
        '6h': 240,  '8h': 330, '12h': 450,'1d': 1000,
        '3d': 3000,'1w': 3000,'1M': 3000
    }

    # All data files go into this single folder
    data_folder = "Data"
    os.makedirs(data_folder, exist_ok=True)

    # Read your symbols list
    with open('Symbols/spot_binance_symbols.csv', 'r') as f:
        symbols = [row[0].strip() for row in csv.reader(f)]

    for interval in intervals:
        # set the global days_gap for the fetch function
        days_gap = days_gap_mapping.get(interval, 1000)
        print(f"\n=== Interval: {interval.upper()} ===")

        for symbol in symbols:
            try:
                process_data_for_symbol(host_url, symbol, interval, data_folder)
            except Exception as e:
                print(f"Error on {symbol}: {e}")
                continue

    # If you want to clean up the CSVs afterwards:
    # process_csv_files(data_folder)
