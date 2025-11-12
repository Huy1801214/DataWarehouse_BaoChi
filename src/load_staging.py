import os
import csv
import uuid
import datetime
from db_connect import connect_to_db


class StagingLoader:
    def __init__(self, db_name="news_staging_db"):
        """Kết nối MySQL thông qua hàm connect_to_db"""
        self.conn = connect_to_db(db_name)
        if not self.conn:
            raise ConnectionError("Không thể kết nối database.")
        self.cursor = self.conn.cursor()

    def clear_staging_table(self):
        """Xóa toàn bộ dữ liệu cũ trong bảng staging_temp_table"""
        print("Đang xóa dữ liệu cũ trong staging_temp_table...")
        self.cursor.execute("DELETE FROM staging_temp_table")
        self.conn.commit()
        print("Đã xóa toàn bộ dữ liệu cũ.")

    def load_csv_to_staging(self, csv_path):
        """Đọc file CSV và nạp vào bảng staging_temp_table"""
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

        # Lấy ngày từ tên file (article_DDMMYY.csv)
        filename = os.path.basename(csv_path)
        try:
            date_str = filename.replace("article_", "").replace(".csv", "")
            date_dim = datetime.datetime.strptime(date_str, "%d%m%y").date()
        except Exception:
            date_dim = datetime.date.today()

        run_id = str(uuid.uuid4())
        print(f"Đang load file: {filename}")
        print(f"run_id: {run_id}")
        print(f"date_dim: {date_dim}")

        insert_query = """
            INSERT INTO staging_temp_table (
                article_url,
                source_name_raw,
                category_raw,
                author_raw,
                published_at_raw,
                title_raw,
                summary_raw,
                content_raw,
                run_id,
                date_dim
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """

        with open(csv_path, "r", encoding="utf-8") as file:
            reader = csv.DictReader(file)
            rows = []
            for row in reader:
                rows.append((
                    str(row.get("article_url", "")),
                    str(row.get("source_name_raw", "")),
                    str(row.get("category_raw", "")),
                    str(row.get("author_raw", "")),
                    str(row.get("published_at_raw", "")),
                    str(row.get("title_raw", "")),
                    str(row.get("summary_raw", "")),
                    str(row.get("content_raw", "")),
                    run_id,
                    date_dim
                ))

            self.cursor.executemany(insert_query, rows)
            self.conn.commit()

        print(f"Đã nạp {len(rows)} bản ghi vào staging_temp_table.")

    def close(self):
        """Đóng kết nối"""
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    # Ví dụ: ../source/article_121125.csv
    csv_file = "../source/article_121125.csv"

    loader = StagingLoader("news_staging_db")
    try:
        loader.clear_staging_table()          # Xóa dữ liệu cũ
        loader.load_csv_to_staging(csv_file)  # Load dữ liệu mới
    finally:
        loader.close()
