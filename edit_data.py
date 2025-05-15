import os
import pandas as pd

# ─── CONFIGURATION ─────────────────────────────────────────────────────────────
FOLDER_PATH = 'Data'  # ← your folder
# ────────────────────────────────────────────────────────────────────────────────

for fname in os.listdir(FOLDER_PATH):
    if not fname.lower().endswith('.csv'):
        continue

    file_path = os.path.join(FOLDER_PATH, fname)
    try:
        df = pd.read_csv(file_path)
    except Exception as e:
        print(f"⚠️  Could not read {fname}: {e}")
        continue

    # rename both 'Open time' → 'timestamp' and 'Open' → 'open'
    rename_map = {}
    if 'Close' in df.columns:
        rename_map['Close'] = 'close'

    if rename_map:
        df.rename(columns=rename_map, inplace=True)
        df.to_csv(file_path, index=False)
        print(f"✅  Renamed {list(rename_map.keys())} in {fname}")
    else:
        print(f"ℹ️  No columns to rename in {fname}, skipping.")

print("🎉 Done.")
