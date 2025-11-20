# from dotenv import load_dotenv
# load_dotenv()

# import os
# import sys
# import uuid
# import time
# import requests
# import pandas as pd
# import mysql.connector

# from datetime import datetime
# from bs4 import BeautifulSoup

# from selenium import webdriver
# from selenium.webdriver.chrome.options import Options

# from utils.logging_manager import LoggingManager

# sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
# from utils.db_utils import get_db_connection


# # ==============================================================================
# # CONSTANTS - SELECTORS
# # ==============================================================================

# SELECTOR_LOOKUP = {
#     'VnExpress': {
#         'selectors': {
#             'article_link': 'h3.title-news a',
#             'tieu_de': 'h1.title-detail',
#             'summary': 'p.description',
#             'content_raw': 'article.fck_detail',
#             'ngay_xuat_ban': 'span.date',
#             'ten_tac_gia': 'p.Normal strong',
#             'tags': 'div.tags h4.item-tag a'
#         }
#     },
#     'TuoiTre': {
#         'selectors': {
#             'article_link': 'h3.box-title-text a',
#             'tieu_de': 'h1.article-title',
#             'summary': 'h2.detail-sapo',
#             'content_raw': 'div.content-detail',
#             'ngay_xuat_ban': 'div.detail-time',
#             'ten_tac_gia': 'div.author-info a.name',
#             'tags': 'div.detail-tab a'
#         }
#     }
# }


# # ==============================================================================
# # SELENIUM SETUP
# # ==============================================================================

# def create_selenium_driver():
#     chrome_options = Options()
#     chrome_options.add_argument("--headless=new")
#     chrome_options.add_argument("--disable-gpu")
#     chrome_options.add_argument("--no-sandbox")
#     chrome_options.add_argument("--disable-dev-shm-usage")
#     chrome_options.add_argument("start-maximized")

#     return webdriver.Chrome(options=chrome_options)


# # ==============================================================================
# # UTILITIES
# # ==============================================================================

# def safe_extract(soup, selector, default='N/A'):
#     """Tránh lỗi khi extract selector."""
#     try:
#         el = soup.select_one(selector)
#         return el.get_text(strip=True) if el else default
#     except:
#         return default


# def normalize_url(url, base_url):
#     """Chuẩn hóa URL."""
#     if url.startswith("http"):
#         return url
#     if url.startswith("//"):
#         return "https:" + url
#     if url.startswith("/"):
#         return base_url + url
#     return f"{base_url}/{url}"


# # ==============================================================================
# # DATABASE CONFIG
# # ==============================================================================

# def fetch_crawl_config_from_db(conn):
#     """Lấy danh sách job crawl từ DB."""
#     query = """
#         SELECT 
#             c.config_id,
#             c.source_name,
#             c.base_url,
#             cat.category_name,
#             cat.category_url
#         FROM config_table c
#         JOIN crawl_Categories cat ON c.config_id = cat.config_id
#         WHERE c.active = TRUE AND cat.active = TRUE;
#     """

#     try:
#         cursor = conn.cursor(dictionary=True)
#         cursor.execute(query)
#         rows = cursor.fetchall()

#         job_list = []
#         for row in rows:
#             selectors = SELECTOR_LOOKUP.get(row['source_name'])
#             if not selectors:
#                 print(f"[WARNING] Thiếu selector cho nguồn: {row['source_name']}")
#                 continue

#             job_list.append({
#                 'config_id': row['config_id'], 
#                 'source_name_raw': row['source_name'],
#                 'start_url': row['category_url'],
#                 'base_url': row['base_url'],
#                 'category_raw': row['category_name'],
#                 'selectors': selectors['selectors']
#             })

#         return job_list

#     except Exception as e:
#         print("[ERROR] Không load được config:", e)
#         return []

#     finally:
#         cursor.close()


# # ==============================================================================
# # PARSE ARTICLE
# # ==============================================================================

# def parse_article_details_selenium(driver, article_url, config, run_id):
#     """Crawl chi tiết bài báo bằng Selenium."""
#     selectors = config['selectors']

#     try:
#         driver.get(article_url)
#         time.sleep(2)

#         soup = BeautifulSoup(driver.page_source, "html.parser")

#         data = {
#             'article_url': article_url,
#             'source_name_raw': config['source_name_raw'],
#             'category_raw': config['category_raw'],
#             'author_raw': safe_extract(soup, selectors['ten_tac_gia']),
#             'published_at_raw': safe_extract(soup, selectors['ngay_xuat_ban']),
#             'title_raw': safe_extract(soup, selectors['tieu_de']),
#             'summary_raw': safe_extract(soup, selectors['summary']),
#             'content_raw': safe_extract(soup, selectors['content_raw']),
#             'scraped_at': datetime.now(),
#             'run_id': run_id
#         }

#         # TAGS
#         tag_elements = soup.select(selectors['tags'])
#         data['tags_raw'] = ", ".join([t.get_text(strip=True) for t in tag_elements]) if tag_elements else "N/A"

#         return data

#     except Exception as e:
#         print(f"[ERROR] Lỗi khi parse bài: {article_url}", e)
#         return None


# # ==============================================================================
# # CRAWL CATEGORY
# # ==============================================================================

# def crawl_site(driver, config, run_id):
#     """Crawl từng category."""
#     try:
#         driver.get(config['start_url'])
#         time.sleep(2)

#         soup = BeautifulSoup(driver.page_source, "html.parser")
#         links = soup.select(config['selectors']['article_link'])

#         results = []
#         for link in links:
#             href = link.get("href")
#             if not href:
#                 continue

#             article_url = normalize_url(href, config['base_url'])
#             article_data = parse_article_details_selenium(driver, article_url, config, run_id)

#             if article_data:
#                 results.append(article_data)

#         return results

#     except Exception as e:
#         print("[ERROR] Lỗi crawl category:", e)
#         return []

# # ==============================================================================
# # LOGGING
# # ==============================================================================

# def log_start(conn, run_id, config_id, job_name):
#     cursor = conn.cursor()
#     query = """
#         INSERT INTO logging_table 
#             (run_id, config_id, job_name, start_time, status, date_dim)
#         VALUES (%s, %s, %s, NOW(), 'RUNNING', CURRENT_DATE)
#     """
#     cursor.execute(query, (run_id, config_id, job_name))
#     conn.commit()
#     cursor.close()


# def log_end(conn, run_id, status, records_extracted, error_message=None):
#     cursor = conn.cursor()
#     query = """
#         UPDATE logging_table
#         SET end_time = NOW(),
#             status = %s,
#             records_extracted = %s,
#             error_message = %s
#         WHERE run_id = %s
#     """
#     cursor.execute(query, (status, records_extracted, error_message, run_id))
#     conn.commit()
#     cursor.close()


# # ==============================================================================
# # MAIN
# # ==============================================================================

# def main():
#     print("\n========== BẮT ĐẦU ==========")
#     run_id = str(uuid.uuid4())

#     conn = get_db_connection()
#     if not conn:
#         print("[ERROR] Không thể kết nối Database")
#         return

#     logger = LoggingManager(conn, run_id)
    
#     jobs = fetch_crawl_config_from_db(conn)
#     if not jobs:
#         print("Không có job nào (Kiểm tra lại bảng config/categories active=1).")
#         return

#     driver = create_selenium_driver()

#     # 1. Tạo một list để chứa TOÀN BỘ dữ liệu từ tất cả các job
#     all_crawled_data = [] 
    
#     total_records = 0
#     error_message = None
#     status = "RUNNING"

#     try:
#         for job in jobs:
#             current_config_id = job['config_id'] 
#             job_name = f"{job['source_name_raw']} - {job['category_raw']}"

#             logger.start(current_config_id, job_name)

#             print(f"\n[Cào] {job_name} (ID: {current_config_id})...")
            
#             # Thực hiện cào dữ liệu
#             data = crawl_site(driver, job, run_id)
#             count = len(data)
            
#             if count > 0:
#                 all_crawled_data.extend(data)
#                 total_records += count
#                 print(f"   -> Tìm thấy {count} bài (Đã thêm vào hàng đợi).")
#             else:
#                 print(f"   -> Không tìm thấy bài nào mới.")

#         # 3. Sau khi vòng lặp kết thúc, kiểm tra và lưu 1 file duy nhất
#         if total_records > 0:
#             df = pd.DataFrame(all_crawled_data)
            
#             date_str = datetime.now().strftime("%d%m%y")      
#             output_dir = f"data/raw/{job['source_name_raw']}"          

#             os.makedirs(output_dir, exist_ok=True)



#             filename = f"{output_dir}/article_{date_str}.csv"
            
#             df.to_csv(filename, index=False, encoding='utf-8-sig')
#             print(f"\n[XUẤT FILE] Đã lưu toàn bộ {total_records} bài viết vào:")
#             print(f" -> {filename}")
        
#         status = "SUCCESS"

#     except Exception as e:
#         status = "FAILED"
#         error_message = str(e)
#         print(f"\n[CRITICAL ERROR]: {error_message}")

#     finally:
#         driver.quit()
#         logger.end(status, total_records, error_message)
#         conn.close()

#     print("\n============== DONE ==============")
#     print("Tổng bài:", total_records)
#     print("Trạng thái:", status)

# if __name__ == "__main__":
#     main()


import sys
import os
import uuid
import time
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# Thêm đường dẫn để import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db_utils import get_db_connection

# ==============================================================================
# CONSTANTS - SELECTORS (CẤU HÌNH CỨNG CHO CÁC TRANG)
# ==============================================================================

SELECTOR_LOOKUP = {
    'VnExpress': {
        'selectors': {
            'article_link': 'h3.title-news a',
            'tieu_de': 'h1.title-detail',
            'summary': 'p.description',
            'content_raw': 'article.fck_detail',
            'ngay_xuat_ban': 'span.date',
            'ten_tac_gia': 'p.Normal strong',
            'tags': 'div.tags h4.item-tag a'
        }
    },
    'TuoiTre': {
        'selectors': {
            'article_link': 'h3.box-title-text a',
            'tieu_de': 'h1.article-title',
            'summary': 'h2.detail-sapo',
            'content_raw': 'div.content-detail',
            'ngay_xuat_ban': 'div.detail-time',
            'ten_tac_gia': 'div.author-info a.name',
            'tags': 'div.detail-tab a'
        }
    },
    # Thêm các trang khác nếu cần
}


# ==============================================================================
# 1. SELENIUM SETUP
# ==============================================================================

def create_selenium_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    return webdriver.Chrome(options=chrome_options)


# ==============================================================================
# 2. UTILITIES
# ==============================================================================

def safe_extract(soup, selector, default='N/A'):
    try:
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else default
    except:
        return default

def normalize_url(url, base_url):
    if url.startswith("http"): return url
    if url.startswith("//"): return "https:" + url
    if url.startswith("/"): return base_url + url
    return f"{base_url}/{url}"


# ==============================================================================
# 3. DATABASE & LOGGING FUNCTIONS (Theo luồng hoạt động)
# ==============================================================================

def get_jobs_from_config(conn):
    """Lấy danh sách các Job trong config (Bước 1 trong sơ đồ)."""
    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                c.config_id,
                c.source_name,
                c.base_url,
                cat.category_name,
                cat.category_url
            FROM config_table c
            JOIN crawl_Categories cat ON c.config_id = cat.config_id
            WHERE c.active = TRUE AND cat.active = TRUE;
        """
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        
        job_list = []
        for row in rows:
            selectors = SELECTOR_LOOKUP.get(row['source_name'])
            if not selectors:
                print(f"[WARNING] Thiếu selector cho nguồn: {row['source_name']}")
                continue
                
            job_list.append({
                'config_id': row['config_id'],
                'source_name_raw': row['source_name'],
                'base_url': row['base_url'],
                'start_url': row['category_url'],
                'category_raw': row['category_name'],
                'selectors': selectors['selectors']
            })
        return job_list
    except Exception as e:
        print(f"[ERROR] Lỗi lấy danh sách Job: {e}")
        return []

def update_logging(conn, run_id, config_id, job_name, status, records_count=0, error_msg=None):
    """Cập nhật bảng logging_table (Các bước cập nhật log trong sơ đồ)."""
    try:
        cursor = conn.cursor()
        
        # Kiểm tra xem log đã tồn tại chưa
        check_query = "SELECT 1 FROM logging_table WHERE run_id = %s AND config_id = %s"
        cursor.execute(check_query, (run_id, config_id))
        exists = cursor.fetchone()

        if not exists:
            # Insert mới (Khi bắt đầu chạy job)
            insert_query = """
                INSERT INTO logging_table (run_id, config_id, job_name, start_time, status, date_dim)
                VALUES (%s, %s, %s, NOW(), %s, CURRENT_DATE)
            """
            cursor.execute(insert_query, (run_id, config_id, job_name, status))
        else:
            # Update (Khi kết thúc job - thành công hoặc thất bại)
            update_query = """
                UPDATE logging_table
                SET end_time = NOW(),
                    status = %s,
                    records_extracted = %s,
                    error_message = %s
                WHERE run_id = %s AND config_id = %s
            """
            cursor.execute(update_query, (status, records_count, error_msg, run_id, config_id))
            
        conn.commit()
        cursor.close()
    except Exception as e:
        print(f"[ERROR] Lỗi ghi log: {e}")


# ==============================================================================
# 4. CRAWLING LOGIC (Chạy script web_scraper.py)
# ==============================================================================

def parse_article(driver, url, config, run_id):
    s = config['selectors']
    try:
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        # Tags logic
        tags = "N/A"
        tag_els = soup.select(s['tags'])
        if tag_els:
            tags = ", ".join([t.get_text(strip=True) for t in tag_els])

        return {
            'article_url': url,
            'source_name_raw': config['source_name_raw'],
            'category_raw': config['category_raw'],
            'author_raw': safe_extract(soup, s['ten_tac_gia']),
            'published_at_raw': safe_extract(soup, s['ngay_xuat_ban']),
            'title_raw': safe_extract(soup, s['tieu_de']),
            'summary_raw': safe_extract(soup, s['summary']),
            'content_raw': safe_extract(soup, s['content_raw']),
            'tags_raw': tags,
            'scraped_at': datetime.now(),
            'run_id': run_id
        }
    except Exception as e:
        print(f"  [Lỗi bài viết] {url}: {e}")
        return None

def run_crawler_for_job(driver, job, run_id):
    """Thực hiện logic crawl cho 1 job cụ thể."""
    print(f" -> Đang crawl: {job['source_name_raw']} - {job['category_raw']}")
    crawled_data = []
    
    try:
        driver.get(job['start_url'])
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        links = soup.select(job['selectors']['article_link'])
        print(f"    Tìm thấy {len(links)} bài viết.")

        # Giới hạn số lượng bài để test (bỏ [:5] để chạy thật)
        for link in links[:5]: 
            href = link.get('href')
            if not href: continue
            
            url = normalize_url(href, job['base_url'])
            data = parse_article(driver, url, job, run_id)
            if data:
                crawled_data.append(data)
                
        return crawled_data, None # (data, error_message)
        
    except Exception as e:
        return [], str(e)


# ==============================================================================
# 5. MAIN WORKFLOW (Khớp với sơ đồ)
# ==============================================================================

def main():
    print("\n========== START WORKFLOW ==========")
    run_id = str(uuid.uuid4())
    
    # 1. Kết nối DB
    conn = get_db_connection()
    if not conn:
        print("[ERROR] Không kết nối được DB. Dừng.")
        return

    # 2. Lấy danh sách Job (Bước: Lấy danh sách các Job trong config)
    jobs = get_jobs_from_config(conn)
    
    # 3. Kiểm tra có Job không? (Bước: Có job không?)
    if not jobs:
        print("[INFO] Không có job nào cần chạy. Kết thúc.")
        conn.close()
        return

    # Khởi tạo driver
    driver = create_selenium_driver()

    # 4. Vòng lặp qua từng Job (Tương ứng với khối "Chạy script")
    for job in jobs:
        config_id = job['config_id']
        job_name = f"{job['source_name_raw']} - {job['category_raw']}"
        
        # Ghi log bắt đầu
        update_logging(conn, run_id, config_id, job_name, "RUNNING")
        
        # Thực thi crawl
        data, error = run_crawler_for_job(driver, job, run_id)
        
        # 5. Kiểm tra kết quả (Bước: Crawl được toàn bộ dữ liệu không?)
        if error:
            # (Bước: Cập nhật logging_table crawl dữ liệu thất bại)
            print(f"  [FAILED] Job {job_name} thất bại: {error}")
            update_logging(conn, run_id, config_id, job_name, "FAILED", 0, error)
        elif not data:
             # Trường hợp chạy không lỗi nhưng không có data
            print(f"  [WARNING] Job {job_name} không có dữ liệu.")
            update_logging(conn, run_id, config_id, job_name, "SUCCESS", 0, "No data found")
        else:
            # 6. Tạo và lưu file (Bước: Tạo và lưu vào file có cấu trúc)
            try:
                # Tạo thư mục: data/raw/{source_name}
                output_dir = os.path.join("source")
                os.makedirs(output_dir, exist_ok=True)
                
                # Tên file: article_DDMMYY.csv
                date_str = datetime.now().strftime("%d%m%y")
                # Thêm run_id hoặc category để tránh ghi đè nếu chạy nhiều lần trong ngày
                filename = f"article_{date_str}.csv"
                # Xử lý ký tự đặc biệt trong tên file (nếu có)
                filename = "".join([c for c in filename if c.isalpha() or c.isdigit() or c in (' ', '.', '_')]).strip()
                
                full_path = os.path.join(output_dir, filename)
                
                df = pd.DataFrame(data)
                df.to_csv(full_path, index=False, encoding='utf-8-sig')
                
                print(f"  [SAVED] Đã lưu {len(data)} dòng vào: {full_path}")
                
                # 7. Cập nhật log thành công (Bước: Cập nhật logging_table crawl thành công)
                update_logging(conn, run_id, config_id, job_name, "SUCCESS", len(data))
                
            except Exception as e:
                print(f"  [ERROR] Lỗi khi lưu file: {e}")
                update_logging(conn, run_id, config_id, job_name, "FAILED", len(data), f"Save file error: {str(e)}")

    # Dọn dẹp
    driver.quit()
    conn.close()
    print("\n========== END WORKFLOW ==========")

if __name__ == "__main__":
    main()