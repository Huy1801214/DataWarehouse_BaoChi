import re
import json
from datetime import datetime
from db_connect import connect_to_db

def transform_articles(data):
    transformed = []
    for item in data:
        clean_title = re.sub(r"<[^>]*>", "", item["title_raw"] or "").strip()
        clean_summary = re.sub(r"<[^>]*>", "", item["summary_raw"] or "").strip()
        clean_author = (item["author_raw"] or "").replace("(tổng hợp)", "").strip()
        clean_category = (item["category_raw"] or "").title().strip()
        clean_source = (item["source_name_raw"] or "").title().strip()

        # Chuyển ngày xuất bản dạng chuỗi -> timestamp
        pub_time = None
        try:
            pub_time = datetime.strptime(item["published_at_raw"], "%d/%m/%Y, %H:%M")
        except Exception:
            pass

        transformed.append({
            "article_url": item["article_url"],
            "source_name": clean_source,
            "category_name": clean_category,
            "author_name": clean_author,
            "published_at": pub_time,
            "title": clean_title,
            "description": clean_summary,
            "word_count": len((item["content_raw"] or "").split()),
            "tags": json.dumps([]),
            "sentiment_score": 0.0,
            "run_id": item["run_id"]
        })
    return transformed


def main():
    conn = connect_to_db("news_staging_db")
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM staging_temp_table")
    raw_data = cursor.fetchall()

    if not raw_data:
        print("Không có dữ liệu trong staging_temp_table.")
        return

    transformed_data = transform_articles(raw_data)

    insert_query = """
        INSERT INTO transformed_temp_table (
            article_url, source_name, category_name, author_name,
            published_at, title, description, word_count, tags,
            sentiment_score, run_id
        ) VALUES (
            %(article_url)s, %(source_name)s, %(category_name)s, %(author_name)s,
            %(published_at)s, %(title)s, %(description)s, %(word_count)s,
            %(tags)s, %(sentiment_score)s, %(run_id)s
        )
    """

    cursor.executemany(insert_query, transformed_data)
    conn.commit()
    print(f"Đã insert {cursor.rowcount} bản ghi vào transformed_temp_table")

    cursor.close()
    conn.close()


if __name__ == "__main__":
    main()
