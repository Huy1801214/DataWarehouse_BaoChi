import re
import json
from datetime import datetime, date
from db_connect import connect_to_db

class TransformLoader:
    def __init__(self, db_name="news_staging_db"):
        self.conn = connect_to_db(db_name)
        if not self.conn:
            raise ConnectionError("Không thể kết nối database.")
        self.cursor = self.conn.cursor(dictionary=True)

    def fetch_staging_data(self):
        self.cursor.execute("SELECT * FROM staging_temp_table")
        return self.cursor.fetchall()

    def clean_text(self, text):
        """Loại bỏ thẻ HTML và trim"""
        if not text:
            return ""
        return re.sub(r"<[^>]*>", "", text).strip()

    def transform_articles(self, data):
        transformed = []
        for item in data:
            article_url = item.get("article_url")
            source_name = (item.get("source_name_raw") or "").replace(".net","").strip().title()
            category_name = (item.get("category_raw") or "").title().strip()
            author_name = (item.get("author_raw") or "").replace("(tổng hợp)", "").strip()
            title = self.clean_text(item.get("title_raw"))
            description = self.clean_text(item.get("summary_raw"))
            content = item.get("content_raw") or ""
            word_count = len(content.split())

            # Chuyển ngày
            published_at = None
            try:
                published_at = datetime.strptime(item.get("published_at_raw",""), "%d/%m/%Y, %H:%M")
            except Exception:
                pass

            transformed.append({
                "article_url": article_url,
                "source_name": source_name,
                "category_name": category_name,
                "author_name": author_name,
                "published_at": published_at,
                "title": title,
                "description": description,
                "word_count": word_count,
                "tags": json.dumps([]),
                "sentiment_score": 0.0,
                "run_id": item.get("run_id"),
                "date_dim": item.get("date_dim") or date.today()
            })
        return transformed

    def clear_today_transformed(self):
        """Xóa dữ liệu cũ trong transformed_temp_table của ngày hôm nay"""
        today = date.today()
        delete_query = "DELETE FROM transformed_temp_table WHERE date_dim = %s"
        self.cursor.execute(delete_query, (today,))
        self.conn.commit()
        print(f"Đã xóa dữ liệu cũ của ngày {today} trong transformed_temp_table")

    def load_transformed_data(self, transformed_data):
        insert_query = """
            INSERT INTO transformed_temp_table (
                article_url, source_name, category_name, author_name,
                published_at, title, description, word_count, tags,
                sentiment_score, run_id, date_dim
            ) VALUES (
                %(article_url)s, %(source_name)s, %(category_name)s, %(author_name)s,
                %(published_at)s, %(title)s, %(description)s, %(word_count)s,
                %(tags)s, %(sentiment_score)s, %(run_id)s, %(date_dim)s
            )
        """
        self.cursor.executemany(insert_query, transformed_data)
        self.conn.commit()
        print(f"Đã insert {self.cursor.rowcount} bản ghi vào transformed_temp_table")

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    loader = TransformLoader()
    try:
        # 1. Xóa dữ liệu cũ hôm nay
        loader.clear_today_transformed()

        # 2. Lấy dữ liệu từ staging
        staging_data = loader.fetch_staging_data()
        if not staging_data:
            print("Không có dữ liệu trong staging_temp_table.")
        else:
            # 3. Transform dữ liệu
            transformed = loader.transform_articles(staging_data)
            # 4. Load vào transformed_temp_table
            loader.load_transformed_data(transformed)
    finally:
        loader.close()
