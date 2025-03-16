from bs4 import BeautifulSoup, element
import urllib
import urllib.request
import math
import csv


from concurrent.futures import ThreadPoolExecutor, as_completed
import threading

import queue

# Hàng đợi để chứa dữ liệu cần ghi
write_queue = queue.Queue()

num_games = 66025 # total games on this website at the time running script
each_queries = 1000

start_page = 1
end_page = math.ceil(num_games / each_queries)

urlhead = 'http://www.vgchartz.com/gamedb/?page='
urltail = '&console=&region=All&developer=&publisher=&genre=&boxart=Both&ownership=Both&order=Sales&ownership=Both&direction=DESC&showtotalsales=1&shownasales=1&showpalsales=1&showjapansales=1&showothersales=1&showpublisher=1&showdeveloper=1&showreleasedate=1&showlastupdate=1&showvgchartzscore=1&showcriticscore=1&showuserscore=1&showshipped=1'
urltail += f'&results={each_queries}'


list_attr = [0, 2, 4, 5]
list_attr.extend(range(6, 17))


with open("./data/vgsales.csv", mode="w", newline="", encoding="utf-8") as file:
    writer = csv.writer(file, delimiter=",")
    writer.writerow([
        "Rank", "Name", "Platform", "Publisher", "Developer", "VGChartz Score",
        "Critic Score", "User Score", "Total Shipped", 
        "Sales NA", "Sales PAL", "Sales JP", "Sales Other", "Sales Global", 
        "Release Date", "Last Update", "Genre"
    ])

    
def fetch_page(page):
    """Tải trang và trích xuất dữ liệu"""
    surl = urlhead + str(page) + urltail
    try:
        r = urllib.request.urlopen(surl).read()
        soup = BeautifulSoup(r, "html.parser")
        print(f"Page: {page}")

        # Lọc các thẻ <a> có chứa link game
        game_tags = list(filter(
            lambda x: x.get('href', '').startswith('https://www.vgchartz.com/game/'),
            soup.find_all("a")
        ))

        return game_tags
    except Exception as e:
        print(f"Error fetching page {page}: {e}")
        return []
    
def extract_value(data, idx):
    """Hàm lấy giá trị từ cột, hỗ trợ chuyển đổi float và cắt ký tự cuối."""
    return data[idx].get_text(strip=True)
    



def get_genre(url):
    
    # try:
    #     # Gửi request và đọc dữ liệu
    #     site_raw = urllib.request.urlopen(url).read()
    #     sub_soup = BeautifulSoup(site_raw, "html.parser")
    #     h2s = sub_soup.find("div", {"id": "gameGenInfoBox"}).find_all("h2")
    #     for h2 in h2s:
    #         if h2.string == "Genre":
    #             return h2.next_sibling.string

    # except Exception as e:
    #     return url
    
    return url


    
def write_worker():
    """Luồng ghi file liên tục lấy dữ liệu từ hàng đợi"""
    with open("./data/vgsales.csv", mode="a", newline="", encoding="utf-8") as file:
        writer = csv.writer(file, delimiter=",")
        
        while True:
            row = write_queue.get()
            if row is None:  # Dấu hiệu dừng luồng
                break
            writer.writerow(row)
            write_queue.task_done()

def process_game_tags(game_tags):
    """Xử lý danh sách game và đẩy vào hàng đợi ghi"""
    for tag in game_tags:
        data = tag.parent.parent.find_all("td")
        platform = data[3].find('img').attrs['alt'] if data[3].find('img') else "Unknown"

        # Lấy dữ liệu theo danh sách cột
        scores_sales = [extract_value(data, i) for i in list_attr]
        rank, gname, publisher, developer, vg_score, critic, user, total_shipped, sales_gl, sales_na, sales_pal, sales_jp, sales_ot, release_date, last_update = scores_sales

        # genre = get_genre(tag.attrs['href'])
        genre = tag.attrs['href']

        # Đẩy dữ liệu vào hàng đợi
        write_queue.put([
            rank, gname, platform, publisher, developer, vg_score,
            critic, user, total_shipped, sales_na, sales_pal, sales_jp, sales_ot, 
            sales_gl, release_date, last_update, genre
        ])

def main():
    # Tạo luồng ghi file riêng biệt
    writer_thread = threading.Thread(target=write_worker, daemon=True)
    writer_thread.start()

    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_game_tags, fetch_page(page)) for page in range(start_page, end_page + 1)]
        for future in as_completed(futures):
            future.result()  # Đảm bảo các luồng xử lý hoàn tất

    # Đợi tất cả dữ liệu được ghi xong
    write_queue.join()
    write_queue.put(None)  # Dừng luồng ghi
    writer_thread.join()

if __name__ == "__main__":
    main()