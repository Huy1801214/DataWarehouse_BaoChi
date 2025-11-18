from dotenv import load_dotenv
load_dotenv()
import os
import uuid
import time
import requests
import mysql.connector
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

# ==============================================================================
# SELENIUM SETUP (DÙNG 1 TRÌNH DUYỆT CHO TOÀN BỘ QUÁ TRÌNH)
# ==============================================================================

def create_selenium_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("start-maximized")

    driver = webdriver.Chrome(options=chrome_options)
    return driver


# ==============================================================================
# KHU VỰC KẾT NỐI DATABASE
# ==============================================================================

def get_database_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database="news_control_db"
        )
        print("[Info] Kết nối CSDL thành công.")
        return conn
    except mysql.connector.Error as e:
        print(f"[LỖI] Không thể kết nối CSDL: {e}")
        return None


# ==============================================================================
# SELECTORS
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
    }
}


# ==============================================================================
# LẤY CONFIG TỪ DATABASE
# ==============================================================================

def fetch_crawl_config_from_db(conn):
    job_list = []
    try:
        cursor = conn.cursor(dictionary=True)
        query = """
            SELECT 
                c.source_name,
                c.base_url,
                cat.category_name,
                cat.category_url
            FROM 
                config_table c
            JOIN 
                crawl_Categories cat ON c.config_id = cat.config_id
            WHERE 
                c.active = TRUE 
                AND cat.active = TRUE;
        """
        cursor.execute(query)
        rows = cursor.fetchall()

        for row in rows:
            source_name = row['source_name']
            lookup = SELECTOR_LOOKUP.get(source_name)

            if not lookup:
                print(f"[WARNING] Không có selector cho nguồn {source_name}")
                continue

            job_list.append({
                'source_name_raw': source_name,
                'start_url': row['category_url'],
                'base_url': row['base_url'],
                'category_raw': row['category_name'],
                'selectors': lookup['selectors']
            })
        return job_list

    except Exception as e:
        print("[Lỗi] Không thể load config:", e)
        return []
    finally:
        cursor.close()


# ==============================================================================
# TIỆN ÍCH
# ==============================================================================

def safe_extract(soup, selector, default='N/A'):
    try:
        el = soup.select_one(selector)
        return el.get_text(strip=True) if el else default
    except:
        return default


def normalize_url(url, base_url):
    if url.startswith("http"):
        return url
    if url.startswith("//"):
        return "https:" + url
    if url.startswith("/"):
        return base_url + url
    return base_url + "/" + url


# ==============================================================================
# PARSE ARTICLE (SỬ DỤNG SELENIUM)
# ==============================================================================

def parse_article_details_selenium(driver, article_url, config, run_id):
    try:
        driver.get(article_url)
        time.sleep(2)  # đợi JS load

        soup = BeautifulSoup(driver.page_source, "html.parser")
        s = config['selectors']

        tieu_de = safe_extract(soup, s["tieu_de"])
        summary = safe_extract(soup, s["summary"])
        content_raw = safe_extract(soup, s["content_raw"])
        ngay_xuat_ban = safe_extract(soup, s["ngay_xuat_ban"])
        ten_tac_gia = safe_extract(soup, s["ten_tac_gia"])

        # TAGS
        tags_str = "N/A"
        try:
            tag_elements = soup.select(s["tags"])
            tags_str = ", ".join([x.get_text(strip=True) for x in tag_elements]) if tag_elements else "N/A"
        except:
            pass

        return {
            'article_url': article_url,
            'source_name_raw': config['source_name_raw'],
            'category_raw': config['category_raw'],
            'author_raw': ten_tac_gia,
            'published_at_raw': ngay_xuat_ban,
            'title_raw': tieu_de,
            'summary_raw': summary,
            'content_raw': content_raw,
            'tags_raw': tags_str,
            'scraped_at': datetime.now(),
            'run_id': run_id
        }

    except Exception as e:
        print(f"[ERROR] Lỗi khi cào bài {article_url}:", e)
        return None


# ==============================================================================
# CRAWL CHUYÊN MỤC
# ==============================================================================

def crawl_site(driver, config, run_id):
    try:
        driver.get(config['start_url'])
        time.sleep(2)

        soup = BeautifulSoup(driver.page_source, "html.parser")
        article_links = soup.select(config['selectors']['article_link'])

        all_data = []

        for link in article_links:
            href = link.get("href")
            if not href:
                continue

            article_url = normalize_url(href, config['base_url'])
            data = parse_article_details_selenium(driver, article_url, config, run_id)
            if data:
                all_data.append(data)

        return all_data

    except Exception as e:
        print("[ERROR] Không thể cào category:", e)
        return []


# ==============================================================================
# MAIN
# ==============================================================================

def main():
    print("========== BẮT ĐẦU ==========")
    run_id = str(uuid.uuid4())

    conn = get_database_connection()
    if not conn:
        return

    jobs = fetch_crawl_config_from_db(conn)
    conn.close()

    if not jobs:
        print("Không có job nào.")
        return

    driver = create_selenium_driver()

    all_data = []

    for job in jobs:
        print("\n[Cào] Nguồn:", job['source_name_raw'], "| Mục:", job['category_raw'])
        data = crawl_site(driver, job, run_id)
        all_data.extend(data)

    driver.quit()

    if not all_data:
        print("Không cào được bài nào.")
        return

    os.makedirs("data/raw", exist_ok=True)
    filename = f"data/raw/crawled_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

    df = pd.DataFrame(all_data)
    df.to_csv(filename, index=False, encoding="utf-8-sig")

    print("\n============== DONE ==============")
    print("Đã cào:", len(all_data), "bài")
    print("Lưu tại:", filename)


if __name__ == "__main__":
    main()
