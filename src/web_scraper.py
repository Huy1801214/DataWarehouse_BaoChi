import sys
import os
import uuid
import time
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end


SELECTOR_LOOKUP = {
    'VnExpress': {
        'selectors': {
            'article_link': 'h3.title-news a',
            'tieu_de': 'h1.title-detail',
            'summary': 'p.description',
            'content_raw': 'article.fck_detail p.Normal',
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
            'content_raw': 'div.detail-content p',
            'ngay_xuat_ban': 'div.detail-time',
            'ten_tac_gia': 'div.author-info a.name',
            'tags': 'div.detail-tab a'
        }
    },
}

def create_selenium_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--dns-prefetch-disable")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.page_load_strategy = 'eager'

    driver = webdriver.Chrome(options=chrome_options)
    driver.set_page_load_timeout(60)
    return driver

def safe_extract(soup, selector, default='N/A'):
    try:
        elements = soup.select(selector)
        
        if len(elements) > 1:
            text_list = [el.get_text(strip=True) for el in elements]
            return "\n".join(text_list)
        elif len(elements) == 1:
            return elements[0].get_text(strip=True)
        else:
            return default
    except:
        return default

def normalize_url(url, base_url):
    if url.startswith("http"): return url
    if url.startswith("//"): return "https:" + url
    if url.startswith("/"): return base_url + url
    return f"{base_url}/{url}"

def get_jobs_from_config(conn):
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

def parse_article(driver, url, config, run_id):
    s = config['selectors']
    try:
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
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
    print(f" -> Đang crawl: {job['source_name_raw']} - {job['category_raw']}")
    crawled_data = []
    
    try:
        driver.get(job['start_url'])
        time.sleep(3)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        
        links = soup.select(job['selectors']['article_link'])
        print(f"    Tìm thấy {len(links)} bài viết.")

        for link in links: 
            href = link.get('href')
            if not href: continue
            
            url = normalize_url(href, job['base_url'])
            data = parse_article(driver, url, job, run_id)
            if data:
                crawled_data.append(data)
                
        return crawled_data, None # (data, error_message)
        
    except Exception as e:
        return [], str(e)

def save_data_to_csv(data):
    try:
        output_dir = os.path.join("source")
        os.makedirs(output_dir, exist_ok=True)

        date_str = datetime.now().strftime("%d%m%y")       
        filename = f"article_{date_str}.csv"      
        full_path = os.path.join(output_dir, filename)  

        df = pd.DataFrame(data)
        df.to_csv(full_path, index=False, encoding='utf-8-sig')
        return full_path
    except Exception as e:
        raise Exception(f"Lỗi khi lưu file CSV: {str(e)}")

def run_all_crawl():
    run_id = str(uuid.uuid4())
    
    # 1. Kết nối tới DB
    conn = connect_to_db("news_control_db")
    if not conn:
        print("[ERROR] Không kết nối được DB. Dừng.")
        return
    
    # 2. Lấy danh sách các Job trong config
    jobs = get_jobs_from_config(conn)
    
    # 2.1. Duyệt xem danh sách các Job còn trống không ?
    if not jobs:
        run_id_sys, _ = log_start("SYSTEM_CHECK", -1)
        log_end(run_id_sys, "FAILED", 0, 0, "There are no jobs to run (Check Active=1)")
        conn.close()
        return

    # 3. Khởi tạo driver
    driver = create_selenium_driver()

    all_data = []
    # 4. Vòng lặp qua từng Job 
    for job in jobs:
        config_id = job['config_id']
        job_name = f"crawl: {job['source_name_raw']}"
        
        # 4.1. Ghi log bắt đầu
        run_id_start, _ = log_start(job_name, config_id)
        
        # 4.2. Thực thi crawl dữ liệu theo từng job
        data, error = run_crawler_for_job(driver, job, run_id_start)
        
        if error:
            # 4.3a. Ghi log "FAILED" do bị lỗi
            log_end(run_id_start, "FAILED", 0, 0, error)
            print(error)
            
        elif not data:
            log_end(run_id_start, "SUCCESS", 0, 0, "No article found")

        else: 
            # 4.3b. Lưu file CSV (cho từng job)
            all_data.extend(data)
            # 4.4. Ghi log SUCCESS
            log_end(run_id_start, "SUCCESS", len(data), len(all_data))
            print(f"  [BUFFER] Đã lấy được {len(data)} bài. Đang chờ lưu...")

    if all_data: 
            try:
                saved_path = save_data_to_csv(all_data)
                print(f"  [SAVED] Đã lưu {len(all_data)} dòng vào: {saved_path}")
            except Exception as e: 
                log_end(run_id_start, "FAILED", 0, 0, error)

    # 5. Thoát Driver
    driver.quit()
    conn.close()

if __name__ == "__main__":
    run_all_crawl()

