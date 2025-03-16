import os
import pandas as pd

# Thư mục chứa các file batch
DATA_DIR = "data"
OUTPUT_FILE = "data/vgsales.csv"

# Tìm tất cả file batch có dạng vgsales_updated_X.csv
csv_files = [f for f in os.listdir(DATA_DIR) if f.startswith("vgsales_updated_") and f.endswith(".csv")]

# Kiểm tra nếu file output đã tồn tại
if not os.path.exists(OUTPUT_FILE):
    print(f"❌ Không tìm thấy {OUTPUT_FILE}. Hãy đảm bảo file này đã được tạo trước!")
    exit(1)

# Đọc file gốc chứa toàn bộ dữ liệu
merged_df = pd.read_csv(OUTPUT_FILE, low_memory=False)
print(f"✅ Đã tải {OUTPUT_FILE} với {len(merged_df)} dòng.")

# Duyệt qua từng file batch để cập nhật Genre vào merged_df
for file in sorted(csv_files, key=lambda x: int(x.split("_")[-1].split(".")[0])):  # Sắp xếp theo batch_id
    file_path = os.path.join(DATA_DIR, file)
    print(f"🔄 Đang cập nhật từ: {file_path}")

    batch_df = pd.read_csv(file_path, usecols=["Rank", "Genre"], low_memory=False)

    # Ghi đè cột Genre vào merged_df theo Rank
    merged_df.update(batch_df.set_index("Rank"), overwrite=True)
    
# Lưu lại file sau khi cập nhật
merged_df.to_csv(OUTPUT_FILE, index=False)
print(f"✅ Đã cập nhật xong! File lưu tại: {OUTPUT_FILE}")
