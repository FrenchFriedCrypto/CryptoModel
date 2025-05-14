import os
import shutil
import pandas as pd

# --- CONFIGURATION ---
QUOTE_VOL_THRESHOLD = 5_500_000
SCAN_FOLDER         = '1d_binance'
OTHER_FOLDERS       = ['4h_binance', '1h_binance']
BACKUP_FOLDER       = 'low_volume_backup'

def main():
    # make a subfolder for the 1d_binance backups
    backup_subdir = os.path.join(BACKUP_FOLDER, SCAN_FOLDER)
    os.makedirs(backup_subdir, exist_ok=True)

    backed_up = 0
    removed_counts = {folder: 0 for folder in OTHER_FOLDERS}

    print(f"\nScanning '{SCAN_FOLDER}' for low-volume files…")
    for fname in os.listdir(SCAN_FOLDER):
        if not fname.lower().endswith('.csv'):
            continue

        one_d_path = os.path.join(SCAN_FOLDER, fname)
        try:
            df = pd.read_csv(one_d_path, usecols=['Volume', 'Close'])
        except Exception as e:
            print(f"  [!] Could not read '{one_d_path}': {e}")
            continue

        avg_quote_vol = (df['Volume'] * df['Close']).mean()
        if avg_quote_vol <= QUOTE_VOL_THRESHOLD:
            # back up into low_volume_backup/1d_binance/<filename>
            dst = os.path.join(backup_subdir, fname)
            shutil.copy2(one_d_path, dst)
            os.remove(one_d_path)  # <— this will delete the 1d file
            backed_up += 1
            print(f"  – Backed up '{fname}'")

            # delete from the other folders
            for folder in OTHER_FOLDERS:
                other_path = os.path.join(folder, fname)
                if os.path.isfile(other_path):
                    os.remove(other_path)
                    removed_counts[folder] += 1
                    print(f"    • Removed from '{folder}'")

    # summary
    print(f"\nBacked up {backed_up} file{'s' if backed_up != 1 else ''} "
          f"to '{backup_subdir}'")
    for folder, cnt in removed_counts.items():
        print(f"Removed {cnt} file{'s' if cnt != 1 else ''} from '{folder}'")

if __name__ == '__main__':
    main()
