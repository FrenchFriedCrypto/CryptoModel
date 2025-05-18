#!/usr/bin/env python3
import pandas as pd
import sys

def filter_final_cash(input_file: str, output_file: str, threshold: float = 10000.0):
    """
    Reads the CSV at input_file, filters rows where 'Final cash' > threshold,
    and writes the result to output_file.
    """
    try:
        # Read the CSV into a DataFrame
        df = pd.read_csv(input_file)
    except FileNotFoundError:
        print(f"Error: '{input_file}' not found.", file=sys.stderr)
        sys.exit(1)
    except pd.errors.EmptyDataError:
        print(f"Error: '{input_file}' is empty or invalid.", file=sys.stderr)
        sys.exit(1)

    # Ensure the 'Final cash' column exists
    if "Final cash" not in df.columns:
        print("Error: Column 'Final cash' not found in the input CSV.", file=sys.stderr)
        sys.exit(1)

    # Filter rows
    filtered_df = df[df["Final cash"] > threshold]

    # Write to output CSV
    filtered_df.to_csv(output_file, index=False)
    print(f"Filtered {len(filtered_df)} rows with Final cash > {threshold} into '{output_file}'.")

if __name__ == "__main__":
    INPUT_CSV = "results_comparison_SOL_SL.csv"
    OUTPUT_CSV = "sol.csv"
    THRESHOLD = 12000.0

    filter_final_cash(INPUT_CSV, OUTPUT_CSV, THRESHOLD)
