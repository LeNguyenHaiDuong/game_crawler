import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import os 

import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0"
]

# Đọc file CSV gốc

# Nếu file đã cập nhật tồn tại, thì đọc vào để tránh ghi đè dữ liệu đã có
try:
    df = pd.read_csv("vgsales_updated.csv")
    print("Đã tải dữ liệu từ vgsales_updated.csv để tiếp tục cập nhật.")
except FileNotFoundError:
    df = pd.read_csv("vgsales.csv")
    print("Không tìm thấy vgsales_updated.csv, sẽ tạo file mới.")

# Kiểm tra cột Genre có tồn tại
if "Genre" not in df.columns:
    raise ValueError("Cột 'Genre' không tồn tại trong file CSV!")

# Biểu thức chính quy kiểm tra URL hợp lệ
url_pattern = re.compile(r"^https?://")

def get_Genre(url):
    headers = {
        "User-Agent": random.choice(USER_AGENTS)
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()  # Kiểm tra lỗi HTTP
        soup = BeautifulSoup(response.text, "html.parser")
        
        # Tìm div chứa thông tin game
        info_box = soup.find("div", {"id": "gameGenInfoBox"})
        if not info_box:
            return "Unknown"  # Nếu không tìm thấy thông tin

        h2s = info_box.find_all("h2")
        for h2 in h2s:
            if h2.string == "Genre":
                return h2.next_sibling.string.strip()

        time.sleep(10)
    
    except:
        print("Cannot get:", url)
        time.sleep(60)
        return url  # Trả về lỗi để xử lý sau

def process_row(idx, url):
    """Hàm xử lý từng dòng, gọi get_Genre với URL."""
    Genre = get_Genre(url)
    return idx, Genre



# Biến đếm số dòng đã xử lý
batch_size = 100
count = 0



# Đọc checkpoint (dòng cuối cùng đã cập nhật)
checkpoint_file = "checkpoint.txt"
if os.path.exists(checkpoint_file):
    with open(checkpoint_file, "r") as f:
        last_index = int(f.read().strip())
else:
    last_index = 0  # Nếu không có checkpoint, bắt đầu từ đầu

# Lọc các dòng cần cập nhật (chỉ cập nhật nếu vẫn là URL)
rows_to_update = [(idx, row["Genre"]) for idx, row in df[last_index:].iterrows() if url_pattern.match(str(row["Genre"]).strip())]


for idx, url in rows_to_update:
    retries = 3  # Số lần thử lại tối đa
    attempt = 0  # Đếm số lần thử
    
    while attempt < retries:
        attempt += 1
        idx, new_Genre = process_row(idx, url)  # Gọi hàm lấy Genre

        if len(new_Genre) > 20:  # Kiểm tra giá trị hợp lệ
            df.at[idx, "Genre"] = new_Genre  # Cập nhật giá trị mới
            count += 1
            break  # Thành công, thoát khỏi vòng lặp
        else:
            print(f"Attempt {attempt}/{retries} failed for {url}. Retrying...")
            time.sleep(5 * attempt)  # Chờ lâu hơn trước mỗi lần thử lại
        

    # Cứ batch_size dòng thì lưu file lại một lần
    if count % batch_size == 0:
        df.to_csv("vgsales_updated.csv", index=False)
        with open(checkpoint_file, "w") as f:
            f.write(str(idx))  # Lưu dòng cuối cùng đã xử lý
        print(f"Saved {count} rows and checkpoint at line {idx}")

# Lưu file lần cuối sau khi hoàn tất
df.to_csv("vgsales_updated.csv", index=False)
print("Hoàn tất cập nhật, dữ liệu đã lưu vào 'vgsales_updated.csv'!")
