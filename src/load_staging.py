import os
import csv
import uuid
from utils.db_utils import connect_to_db

class StagingLoader:
    """Chỉ chịu trách nhiệm load CSV vào bảng staging_temp_table."""

    def __init__(self, db_name="news_staging_db"):
        self.conn = connect_to_db(db_name)
        self.cursor = self.conn.cursor()

    def clear_staging_table(self):
        self.cursor.execute("DELETE FROM staging_temp_table")
        self.conn.commit()
        print("Đã xóa toàn bộ dữ liệu cũ trong staging_temp_table.")

    def load_csv_to_staging(self, csv_path):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

        run_id = str(uuid.uuid4())
        rows = []

        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append((
                    row.get("article_url", ""),
                    row.get("source_name_raw", ""),
                    row.get("category_raw", ""),
                    row.get("author_raw", ""),
                    row.get("published_at_raw", ""),
                    row.get("title_raw", ""),
                    row.get("summary_raw", ""),
                    row.get("content_raw", ""),
                    row.get("tags_raw", ""),
                    run_id
                ))

        if not rows:
            print("CSV không có dữ liệu để nạp vào DB.")
            return False

        insert_query = """
            INSERT INTO staging_temp_table (
                article_url, source_name_raw, category_raw, author_raw,
                published_at_raw, title_raw, summary_raw, content_raw,
                tags_raw, run_id
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        try:
            self.cursor.executemany(insert_query, rows)
            self.conn.commit()
            print(f"Đã nạp {len(rows)} bản ghi vào staging_temp_table với run_id {run_id}.")
            return True
        except Exception as e:
            print("Lỗi khi nạp dữ liệu vào staging_temp_table:", str(e))
            self.conn.rollback()
            return False

    def close(self):
        self.cursor.close()
        self.conn.close()
        
if __name__ == "__main__":
    csv_file = "./source/article_20251115.csv"  # Đường dẫn tới CSV

    loader = StagingLoader("news_staging_db")
    try:
        #  Xóa dữ liệu cũ
        loader.clear_staging_table()

        #  Load dữ liệu mới từ CSV
        success = loader.load_csv_to_staging(csv_file)
        if not success:
            print("Không load được dữ liệu CSV.")
    finally:
        #  Đóng kết nối DB
        loader.close()
