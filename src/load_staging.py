import os
import csv
import uuid
from datetime import datetime
from utils.db_utils import connect_to_db

class StagingLoader:
    """Load CSV vào staging_temp_table và ghi log vào logging_table, tự động quản lý config_id."""

    def __init__(self, staging_db="news_staging_db", control_db="news_control_db"):
        self.staging_conn = connect_to_db(staging_db)
        self.staging_cursor = self.staging_conn.cursor()
        
        self.control_conn = connect_to_db(control_db)
        self.control_cursor = self.control_conn.cursor()
        
        self.run_id = str(uuid.uuid4())
        self.config_id = None
    
    # =============================
    # Config helper
    # =============================
    def get_or_create_config(self, source_name):
        # Kiểm tra config đã tồn tại
        select_query = "SELECT config_id FROM config_table WHERE source_name=%s"
        self.control_cursor.execute(select_query, (source_name,))
        row = self.control_cursor.fetchone()
        if row:
            self.config_id = row[0]
        else:
            # Tạo mới config
            insert_query = """
                INSERT INTO config_table (source_name, active, updated_at)
                VALUES (%s, %s, %s)
            """
            self.control_cursor.execute(insert_query, (source_name, True, datetime.now()))
            self.control_conn.commit()
            self.config_id = self.control_cursor.lastrowid
        return self.config_id
    
    # =============================
    # Log helper
    # =============================
    def create_log(self, job_name="staging_loader"):
        insert_log = """
            INSERT INTO logging_table (
                run_id, config_id, job_name, status, start_time
            ) VALUES (%s,%s,%s,%s,%s)
        """
        self.control_cursor.execute(insert_log, (self.run_id, self.config_id, job_name, "running", datetime.now()))
        self.control_conn.commit()
    
    def update_log(self, status="SUCCESS", records_loaded=0, error_message=None):
        update_log = """
            UPDATE logging_table
            SET status=%s,
                end_time=%s,
                records_loaded=%s,
                error_message=%s
            WHERE run_id=%s
        """
        self.control_cursor.execute(update_log, (status, datetime.now(), records_loaded, error_message, self.run_id))
        self.control_conn.commit()
    
    # =============================
    # Load CSV
    # =============================
    def clear_staging_table(self):
        self.staging_cursor.execute("DELETE FROM staging_temp_table")
        self.staging_conn.commit()
        print("Đã xóa toàn bộ dữ liệu cũ trong staging_temp_table.")

    def load_csv_to_staging(self, csv_path):
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

        # Lấy source_name từ dòng đầu tiên CSV để tạo/get config
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            first_row = next(reader, None)
            if not first_row:
                print("CSV không có dữ liệu.")
                return False
            source_name = first_row.get("source_name_raw", "unknown_source")
            self.get_or_create_config(source_name)
        
        # Tạo log ban đầu
        self.create_log()

        rows = []
        try:
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
                self.update_log(status="partial", records_loaded=0, error_message="CSV không có dữ liệu")
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
            self.update_log(status="SUCCESS", records_loaded=len(rows))
            print(f"Đã nạp {len(rows)} bản ghi vào staging_temp_table với run_id {self.run_id}.")
            return True

        except Exception as e:
            self.staging_conn.rollback()
            self.update_log(status="FAILED", error_message=str(e))
            print("Lỗi khi nạp dữ liệu vào staging_temp_table:", str(e))
            return False

    # =============================
    # Close connections
    # =============================
    def close(self):
        self.staging_cursor.close()
        self.staging_conn.close()
        self.control_cursor.close()
        self.control_conn.close()


if __name__ == "__main__":
    csv_file = "./source/article_201125.csv"  # Đường dẫn tới CSV

    loader = StagingLoader()
    try:
        loader.clear_staging_table()
        SUCCESS = loader.load_csv_to_staging(csv_file)
        if not SUCCESS:
            print("Không load được dữ liệu CSV.")
    finally:
        loader.close()
