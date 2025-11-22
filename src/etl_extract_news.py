import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime
from db_connect import connect_to_db 


# ========== Crawl dữ liệu từ VNExpress ==========
def crawl_vnexpress(limit=5):
    url = "https://vnexpress.net/"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    articles = []
    for item in soup.select("article.item-news", limit=limit):
        title_tag = item.select_one("h3.title-news a")
        summary_tag = item.select_one("p.description a")
        if not title_tag:
            continue

        article_url = title_tag.get("href", "")
        title = title_tag.text.strip()
        summary = summary_tag.text.strip() if summary_tag else ""
        source = "VnExpress"
        published_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        articles.append({
            "article_url": article_url,
            "source_name_raw": source,
            "category_raw": "Trang chủ",
            "author_raw": "",
            "published_at_raw": published_at,
            "title_raw": title,
            "summary_raw": summary,
            "content_raw": ""
        })
    return pd.DataFrame(articles)


# ========== Lưu dữ liệu vào DB (dùng connect_to_db) ==========
def save_to_staging(df):
    conn = connect_to_db("news_staging_db")
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO staging_temp_table 
        (article_url, source_name_raw, category_raw, author_raw, 
         published_at_raw, title_raw, summary_raw, content_raw)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
    """

    for _, row in df.iterrows():
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    conn.close()
    print(f" Đã lưu {len(df)} bài báo vào staging_temp_table")


# ========== Chạy toàn bộ ==========
if __name__ == "__main__":
    df = crawl_vnexpress(limit=10)
    print(df.head())
    save_to_staging(df)s