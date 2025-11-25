import os
import csv
import glob
from src.utils.db_utils import connect_to_db
from src.utils.log_utils import log_start, log_end

# =============================================
# STAGING LOADER CLASS
# Mục đích: Load dữ liệu CSV vào staging_temp_table và quản lý logging
# =============================================
class StagingLoader:
    """Load CSV vào staging_temp_table, quản lý run_id tự động với logging."""

    def __init__(self, staging_db="news_staging_db", job_name="Load_Staging"):
        """
        Khởi tạo StagingLoader
        """
        # 1: Kết nối database
        self.staging_conn = connect_to_db(staging_db)
        
        # 2: Tạo cursor
        self.staging_cursor = self.staging_conn.cursor()
        self.job_name = job_name
        
        # 3: Log START và lấy run_id
        self.run_id, _ = log_start(job_name)
        print(f"[INFO] RUN_ID: {self.run_id}")

    # =============================
    # Clear staging table
    # Mục đích: Xóa toàn bộ dữ liệu cũ trong staging_temp_table
    # =============================
    def clear_staging_table(self):
        """
        Xóa dữ liệu cũ trong staging_temp_table
        """
        # 4: Execute DELETE query
        self.staging_cursor.execute("DELETE FROM staging_temp_table")
        
        # 5: Commit transaction
        self.staging_conn.commit()
        print("Đã xóa toàn bộ dữ liệu cũ trong staging_temp_table.")

    # =============================
    # Load CSV vào staging
    # Mục đích: Đọc file CSV và insert vào staging_temp_table
    # =============================
    def load_csv_to_staging(self, csv_path):
        """
        Load dữ liệu từ CSV vào staging_temp_table
        """
        total_rows = 0
        try:
            # 6: Kiểm tra file tồn tại
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

            # 7: Đọc dữ liệu từ CSV
            rows = []
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                # 7.1: Lặp qua từng row và chuẩn bị dữ liệu
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

            # 7.2: Kiểm tra CSV có dữ liệu không
            if not rows:
                print("CSV không có dữ liệu.")
                log_end(self.run_id, "SUCCESS", total_rows, total_rows)
                return False

            # 8: Insert dữ liệu vào database
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
            # 8.1: Execute insert nhiều rows
            self.staging_cursor.executemany(insert_query, rows)
            
            # 8.2: Commit transaction
            self.staging_conn.commit()
            total_rows = len(rows)
            print(f"Đã nạp {total_rows} bản ghi vào staging_temp_table với run_id {self.run_id}.")
            
            # 9: Log kết quả SUCCESS
            log_end(self.run_id, "SUCCESS", total_rows, total_rows)
            return True

        except Exception as e:
            # 9.1: Rollback và log FAILED
            self.staging_conn.rollback()
            print("Lỗi khi nạp dữ liệu vào staging_temp_table:", str(e))
            log_end(self.run_id, "FAILED", total_rows, 0, str(e))
            return False

    # =============================
    # Đóng kết nối
    # Mục đích: Giải phóng tài nguyên database
    # =============================
    def close(self):
        """
        Đóng kết nối database
        """
        # 10: Đóng cursor
        self.staging_cursor.close()
        
        # 11: Đóng connection
        self.staging_conn.close()

if __name__ == "__main__":
    # Tìm file CSV mới nhất trong thư mục source
    list_of_files = glob.glob('./source/article_*.csv')

    if not list_of_files:
        print("Không tìm thấy file CSV nào trong thư mục source!")
    else:
        # Lấy file mới nhất dựa trên thời gian tạo
        latest_file = max(list_of_files, key=os.path.getctime)
        print(f"Phát hiện file mới nhất: {latest_file}")
        
        # Khởi tạo StagingLoader
        loader = StagingLoader()
        try:
            #  Xóa dữ liệu cũ
            loader.clear_staging_table()
            
            #  Load CSV vào staging
            loader.load_csv_to_staging(latest_file)
        finally:
            #16: Đóng kết nối (luôn chạy dù có lỗi hay không)
            loader.close()
