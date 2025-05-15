import os
import shutil

# ← EDIT THESE to match your setup:
source_folders = [
    r'4h_binance',
    r'1h_binance',
    r'1d_binance',
]
target_folder = r'Data'

def consolidate_csvs(sources, target):
    # Ensure target exists
    os.makedirs(target, exist_ok=True)

    for folder in sources:
        if not os.path.isdir(folder):
            print(f"[!] Source folder not found, skipping: {folder}")
            continue

        for fname in os.listdir(folder):
            if not fname.lower().endswith('.csv'):
                continue

            src_path = os.path.join(folder, fname)
            dst_path = os.path.join(target, fname)

            if os.path.exists(dst_path):
                print(f"→ Skipping (already exists): {fname}")
            else:
                shutil.copy2(src_path, dst_path)
                print(f"Copied: {folder}/{fname} → {target}/{fname}")

if __name__ == '__main__':
    consolidate_csvs(source_folders, target_folder)
