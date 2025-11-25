import os
import csv
import glob
from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end

# =============================================
# STAGING LOADER CLASS
# Mục đích: Load dữ liệu CSV vào staging_temp_table và quản lý logging
# =============================================
class StagingLoader:
    """Load CSV vào staging_temp_table, quản lý run_id tự động với logging."""

    def __init__(self, staging_db="news_staging_db", job_name="Load_Staging"):
        """
        Khởi tạo StagingLoader
        Bước 2.1: Kết nối database
        Bước 2.2: Tạo cursor
        Bước 2.3: Log bắt đầu job và lấy run_id
        """
        # Bước 2.1: Kết nối database
        self.staging_conn = connect_to_db(staging_db)
        
        # Bước 2.2: Tạo cursor
        self.staging_cursor = self.staging_conn.cursor()
        self.job_name = job_name
        
        # Bước 2.3: Log START và lấy run_id
        self.run_id, _ = log_start(job_name)
        print(f"[INFO] RUN_ID: {self.run_id}")

    # =============================
    # Bước 2.4: Clear staging table
    # Mục đích: Xóa toàn bộ dữ liệu cũ trong staging_temp_table
    # =============================
    def clear_staging_table(self):
        """
        Xóa dữ liệu cũ trong staging_temp_table
        Bước 2.4.1: Execute DELETE query
        Bước 2.4.2: Commit transaction
        """
        # Bước 2.4.1: Execute DELETE query
        self.staging_cursor.execute("DELETE FROM staging_temp_table")
        
        # Bước 2.4.2: Commit transaction
        self.staging_conn.commit()
        print("Đã xóa toàn bộ dữ liệu cũ trong staging_temp_table.")

    # =============================
    # Bước 2.5: Load CSV vào staging
    # Mục đích: Đọc file CSV và insert vào staging_temp_table
    # =============================
    def load_csv_to_staging(self, csv_path):
        """
        Load dữ liệu từ CSV vào staging_temp_table
        Bước 2.5.1: Kiểm tra file tồn tại
        Bước 2.5.2: Đọc dữ liệu từ CSV
        Bước 2.5.3: Insert dữ liệu vào database
        Bước 2.5.4: Log kết quả
        """
        total_rows = 0
        try:
            # Bước 2.5.1: Kiểm tra file tồn tại
            if not os.path.exists(csv_path):
                raise FileNotFoundError(f"Không tìm thấy file: {csv_path}")

            # Bước 2.5.2: Đọc dữ liệu từ CSV
            rows = []
            with open(csv_path, "r", encoding="utf-8-sig") as f:
                reader = csv.DictReader(f)
                # Bước 2.5.2.1: Lặp qua từng row và chuẩn bị dữ liệu
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

            # Bước 2.5.2.2: Kiểm tra CSV có dữ liệu không
            if not rows:
                print("CSV không có dữ liệu.")
                log_end(self.run_id, "SUCCESS", total_rows, total_rows)
                return False

            # Bước 2.5.3: Insert dữ liệu vào database
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
            # Bước 2.5.3.1: Execute insert nhiều rows
            self.staging_cursor.executemany(insert_query, rows)
            
            # Bước 2.5.3.2: Commit transaction
            self.staging_conn.commit()
            total_rows = len(rows)
            print(f"Đã nạp {total_rows} bản ghi vào staging_temp_table với run_id {self.run_id}.")
            
            # Bước 2.5.4: Log kết quả SUCCESS
            log_end(self.run_id, "SUCCESS", total_rows, total_rows)
            return True

        except Exception as e:
            # Bước 2.5.4 (nhánh lỗi): Rollback và log FAILED
            self.staging_conn.rollback()
            print("Lỗi khi nạp dữ liệu vào staging_temp_table:", str(e))
            log_end(self.run_id, "FAILED", total_rows, 0, str(e))
            return False

    # =============================
    # Bước 2.6: Đóng kết nối
    # Mục đích: Giải phóng tài nguyên database
    # =============================
    def close(self):
        """
        Đóng kết nối database
        Bước 2.6.1: Đóng cursor
        Bước 2.6.2: Đóng connection
        """
        # Bước 2.6.1: Đóng cursor
        self.staging_cursor.close()
        
        # Bước 2.6.2: Đóng connection
        self.staging_conn.close()


# =============================================
# MAIN EXECUTION
# Điểm khởi chạy chương trình
# =============================================
if __name__ == "__main__":
    # Bước 2.7: Tìm file CSV mới nhất trong thư mục source
    list_of_files = glob.glob('./source/article_*.csv')

    if not list_of_files:
        # Bước 2.7.1: Không tìm thấy file CSV
        print("Không tìm thấy file CSV nào trong thư mục source!")
    else:
        # Bước 2.7.2: Lấy file mới nhất dựa trên thời gian tạo
        latest_file = max(list_of_files, key=os.path.getctime)
        
        print(f"Phát hiện file mới nhất: {latest_file}")
        
        # Bước 2.8: Khởi tạo StagingLoader
        loader = StagingLoader()
        try:
            # Bước 2.9: Xóa dữ liệu cũ
            loader.clear_staging_table()
            
            # Bước 2.10: Load CSV vào staging
            loader.load_csv_to_staging(latest_file)
        finally:
            # Bước 2.11: Đóng kết nối (luôn chạy dù có lỗi hay không)
            loader.close()