import pandas as pd
import requests
from bs4 import BeautifulSoup
import time
import random
import sys

batch_id = int(sys.argv[1])  # Nhận batch_id từ GitHub Actions
batch_size = int(sys.argv[2])  # Mỗi workflow xử lý 10.000 dòng

# Xác định phạm vi dòng cần xử lý
start_idx = batch_id * batch_size
end_idx = start_idx + batch_size

# Đọc và ghi từ file này
output_file = f"data/vgsales_updated_{batch_id}.csv"

USER_AGENTS = [
    # 🌐 Chrome (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.88 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.5938.132 Safari/537.36",

    # 🍏 Chrome (MacOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",

    # 🐧 Chrome (Linux)
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",

    # 🔥 Firefox (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:119.0) Gecko/20100101 Firefox/119.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:118.0) Gecko/20100101 Firefox/118.0",

    # 🍏 Firefox (MacOS)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7; rv:120.0) Gecko/20100101 Firefox/120.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_6; rv:119.0) Gecko/20100101 Firefox/119.0",

    # 🏢 Edge (Windows)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36 Edg/119.0.0.0",

    # 📱 Mobile (iPhone)
    "Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 15_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.6 Mobile/15E148 Safari/604.1",

    # 📱 Mobile (Android)
    "Mozilla/5.0 (Linux; Android 13; SM-S908B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36",
    "Mozilla/5.0 (Linux; Android 12; Pixel 6 Pro) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Mobile Safari/537.36",

    # 🤖 Googlebot (Crawler)
    "Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",
    "Mozilla/5.0 (Linux; Android 10; SM-G975F) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Mobile Safari/537.36 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)",

    # 🕵️ Bingbot (Crawler)
    "Mozilla/5.0 (compatible; bingbot/2.0; +http://www.bing.com/bingbot.htm)",
]


try:
    # Đọc file CSV gốc
    print("Đang tải dữ liệu từ vgsales_updated.csv để tiếp tục cập nhật.")
    cols_to_read = ["Rank", "Genre"]  # Chỉ đọc cột cần thiết
    df = pd.read_csv(output_file, usecols=cols_to_read, low_memory=False)
    print(f"✅ Processing Batch {batch_id}: Rows {start_idx} to {end_idx}")
except FileNotFoundError:
    # Nếu file đã cập nhật tồn tại, thì đọc vào để tránh ghi đè dữ liệu đã có
    df = pd.read_csv("vgsales_updated.csv", low_memory=False)
    df = df.iloc[start_idx:end_idx]
    print("Không tìm thấy vgsales_updated.csv, sẽ tạo file mới.")

# Kiểm tra cột Genre có tồn tại
if "Genre" not in df.columns:
    raise ValueError("Cột 'Genre' không tồn tại trong file CSV!")

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

# Lọc các dòng cần cập nhật (chỉ cập nhật nếu vẫn là URL)
rows_to_update = [(idx, row["Genre"]) for idx, row in df.iterrows() if len(row["Genre"]) > 20]
if len(rows_to_update) == 0:
    print("This batch is already done.")
else:
    print(f"Done filtering, update list from {rows_to_update[0]}")


batch_size_update = 100
count = 0

for idx, url in rows_to_update:
    idx, new_Genre = process_row(idx, url)  # Gọi hàm lấy Genre
    while (len(new_Genre) > 20):
        df.to_csv(output_file, index=False)
        print(f"Saved until line {idx}")
        print("Retry in 180s")
        time.sleep(180)
        idx, new_Genre = process_row(idx, url)  # Gọi hàm lấy Genre

    print(f"Update indx {idx}")
    df.at[idx, "Genre"] = new_Genre  # Cập nhật giá trị mới
    count += 1

    if count == batch_size_update:
        count = 0
        df.to_csv(output_file, index=False)
        print(f"Saved until line {idx}")

df.to_csv(output_file, index=False)
print(f"✅ Saved {output_file}")


