import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

import random

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0"
]

try:
    # Đọc file CSV gốc
    df = pd.read_csv("vgsales_updated.csv", low_memory=False)
    print("Đã tải dữ liệu từ vgsales_updated.csv để tiếp tục cập nhật.")
except FileNotFoundError:
    # Nếu file đã cập nhật tồn tại, thì đọc vào để tránh ghi đè dữ liệu đã có
    df = pd.read_csv("vgsales.csv", low_memory=False)
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
    
    except:
        print("Cannot get:", url)
        return url  # Trả về lỗi để xử lý sau

def process_row(idx, url):
    """Hàm xử lý từng dòng, gọi get_Genre với URL."""
    Genre = get_Genre(url)
    return idx, Genre



# Biến đếm số dòng đã xử lý
batch_size = 30
count = 0


# Lọc các dòng cần cập nhật (chỉ cập nhật nếu vẫn là URL)
rows_to_update = [(idx, row["Genre"]) for idx, row in df.iterrows() if url_pattern.match(str(row["Genre"]).strip())]
print(f"Done filtering, update list from {rows_to_update[0]}")

for idx, url in rows_to_update:
    idx, new_Genre = process_row(idx, url)  # Gọi hàm lấy Genre
    while (len(new_Genre) > 20):
        print("Retry in 180s")
        time.sleep(180)
        idx, new_Genre = process_row(idx, url)  # Gọi hàm lấy Genre

    print(f"Update indx {idx}")
    df.at[idx, "Genre"] = new_Genre  # Cập nhật giá trị mới
    count += 1
        

    # Cứ batch_size dòng thì lưu file lại một lần
    if count == batch_size:
        df.to_csv("vgsales_updated.csv", index=False)
        print(f"Saved {count} rows and checkpoint at line {idx}")
        count = 0
        time.sleep(180)

# Lưu file lần cuối sau khi hoàn tất
df.to_csv("vgsales_updated.csv", index=False)
print("Hoàn tất cập nhật, dữ liệu đã lưu vào 'vgsales_updated.csv'!")
