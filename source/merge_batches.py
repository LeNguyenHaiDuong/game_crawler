import os
import pandas as pd


DATA_DIR = "./data"
OUTPUT_FILE = os.path.join(DATA_DIR, "vgsales.csv")

# Tìm tất cả batch files
csv_files = sorted(
    [os.path.join(DATA_DIR, f) for f in os.listdir(DATA_DIR) if f.startswith("vgsales_batch_")]
)

print(csv_files)

if not csv_files:
    print("⚠️ Không có batch nào để gộp!")
    exit(1)

# Gộp dữ liệu từ tất cả batch
dfs = []

for i, file in enumerate(csv_files):
    try:
        if i == 0:
            # File đầu tiên giữ nguyên cột
            df = pd.read_csv(file, low_memory=False)
        else:
            # Các file sau bỏ dòng tiêu đề
            df = pd.read_csv(file, low_memory=False, header=None, skiprows=1)
            df.columns = dfs[0].columns  # Đặt lại tên cột theo file đầu tiên
        
        dfs.append(df)
    except Exception as e:
        print(f"⚠️ Bỏ qua file {file} do lỗi: {e}")
        
merged_df = pd.concat(dfs, ignore_index=True)

# Loại bỏ trùng lặp theo Rank (giữ giá trị mới nhất)
merged_df.drop_duplicates(subset=["Rank"], keep="last", inplace=True)

# Lưu file gộp
merged_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Đã gộp xong! File lưu tại: {OUTPUT_FILE}")
