import os
import pandas as pd

# Thư mục chứa các file batch
DATA_DIR = "./data"
INPUT_FILE = "./data/vgsales.csv"
OUTPUT_FILE = "./data/vgsales_final.csv"


# Tìm tất cả file batch có dạng vgsales_updated_X.csv
csv_files = [f for f in os.listdir(DATA_DIR) if f.startswith("vgsales_updated_") and f.endswith(".csv")]

# Kiểm tra nếu file output đã tồn tại
if not os.path.exists(INPUT_FILE):
    print(f"❌ Không tìm thấy {INPUT_FILE}. Hãy đảm bảo file này đã được tạo trước!")
    exit(1)

# Đọc file gốc chứa toàn bộ dữ liệu
merged_df = pd.read_csv(INPUT_FILE, low_memory=False)
print(f"✅ Đã tải {INPUT_FILE} với {len(merged_df)} dòng.")

# Danh sách chứa dữ liệu từ tất cả batch
batch_data = []

# 🔹 Bước 1: Đọc tất cả file batch và gộp thành một DataFrame
for file in sorted(csv_files, key=lambda x: int(x.split("_")[-1].split(".")[0])):  # Sắp xếp theo batch_id
    file_path = os.path.join(DATA_DIR, file)
    print(f"🔄 Đang đọc file: {file_path}")
    
    batch_df = pd.read_csv(file_path, usecols=["Rank", "Genre"], low_memory=False)
    batch_data.append(batch_df)

# Nếu có batch để cập nhật, thực hiện merge
if batch_data:
    batch_df = pd.concat(batch_data, ignore_index=True)

    # 🔹 Bước 2: Loại bỏ dòng trùng lặp theo Rank (giữ giá trị cuối cùng)
    batch_df.drop_duplicates(subset=["Rank"], keep="last", inplace=True)

    # 🔹 Bước 3: Ghi giá trị Genre mới vào `merged_df` theo Rank
    merged_df = merged_df.merge(batch_df, on="Rank", how="left", suffixes=("", "_new"))
    
    # Chỉ cập nhật những dòng có giá trị mới
    merged_df["Genre"] = merged_df["Genre_new"].combine_first(merged_df["Genre"])

    # Xóa cột tạm
    merged_df.drop(columns=["Genre_new"], inplace=True)

    # Lưu lại file sau khi cập nhật
    merged_df.to_csv(OUTPUT_FILE, index=False)
    print(f"✅ Đã cập nhật xong! File lưu tại: {OUTPUT_FILE}")
else:
    print("⚠️ Không có file batch nào để cập nhật!")
