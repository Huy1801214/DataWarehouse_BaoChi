import os
import csv
import uuid
from utils.db_utils import connect_to_db, log_startg, log_endg

class StagingLoader:
    """Load CSV vào staging_temp_table, quản lý run_id tự động với logging."""

    def __init__(self, staging_db="news_staging_db", job_name="Load_Staging"):
        self.staging_conn = connect_to_db(staging_db)
        self.staging_cursor = self.staging_conn.cursor()
        self.job_name = job_name
        # Log START
        self.run_id, _ = log_startg(job_name)
        print(f"[INFO] RUN_ID: {self.run_id}")

    # =============================
    # Clear staging table
    # =============================
    def clear_staging_table(self):
        self.staging_cursor.execute("DELETE FROM staging_temp_table")
        self.staging_conn.commit()
        print("Đã xóa toàn bộ dữ liệu cũ trong staging_temp_table.")

    # =============================
    # Load CSV
    # =============================
    def load_csv_to_staging(self, csv_path):
        total_rows = 0
        try:
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

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
                        row.get("scraped_at", ""),
                        self.run_id,
                        row.get("tags_raw", "")
                    ))

            if not rows:
                print("CSV không có dữ liệu.")
                log_endg(self.run_id, "SUCCESS", total_rows, total_rows)
                return False

            insert_query = """
                INSERT INTO staging_temp_table (
                    article_url,
                    source_name,
                    category,
                    author,
                    published_at,
                    title,
                    summary,
                    content,
                    scraped_at,
                    run_id,
                    tags
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """
            self.staging_cursor.executemany(insert_query, rows)
            self.staging_conn.commit()
            total_rows = len(rows)
            print(f"Đã nạp {total_rows} bản ghi vào staging_temp_table với run_id {self.run_id}.")
            log_endg(self.run_id, "SUCCESS", total_rows, total_rows)
            return True

        except Exception as e:
            self.staging_conn.rollback()
            print("Lỗi khi nạp dữ liệu vào staging_temp_table:", str(e))
            log_endg(self.run_id, "FAILED", total_rows, 0, str(e))
            return False

    # =============================
    # Close connections
    # =============================
    def close(self):
        self.staging_cursor.close()
        self.staging_conn.close()


if __name__ == "__main__":
    csv_file = "./source/article_201125.csv"  # Đường dẫn tới CSV

    loader = StagingLoader()
    try:
        loader.clear_staging_table()
        success = loader.load_csv_to_staging(csv_file)
        if not success:
            print("Không load được dữ liệu CSV.")
    finally:
        loader.close()
