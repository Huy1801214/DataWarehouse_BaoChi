from src.utils.db_utils import connect_to_db
from src.utils.log_utils import log_start, log_end
from bs4 import BeautifulSoup
import re
from pyvi import ViTokenizer


# =============================================
# SIÊU HÀM FIX DÍNH TỪ TIẾNG VIỆT
# Mục đích: Tách từ tiếng Việt và xử lý các trường hợp dính từ
# =============================================
def advanced_vietnamese_spacing(text: str) -> str:
    """
    Xử lý khoảng trắng và tách từ tiếng Việt
    Input: text - Chuỗi văn bản tiếng Việt cần xử lý
    Output: Chuỗi đã được tách từ và chuẩn hóa khoảng trắng
    """
    # Kiểm tra input rỗng
    if not text or not text.strip():
        return ""
    
    # Chuẩn hóa dấu nháy đơn và nháy kép
    text = text.replace("'", "'").replace("'", "'").replace(""", '"').replace(""", '"')
    
    # Tách từ tiếng Việt bằng ViTokenizer và thay thế dấu gạch dưới
    text = ViTokenizer.tokenize(text).replace("_", " ")
    
    # Xử lý các trường hợp dính từ (chữ thường dính chữ hoa)
    text = re.sub(r"([a-zàáạảãâăêôơưéèẻẽẹđỳỳỷỹ])([A-ZÀÁẠẢÃÂĂÊÔƠƯÉÈẺẼẸĐỲỶỸ])", r"\1 \2", text)
    
    # Chuẩn hóa nhiều khoảng trắng thành 1 khoảng trắng
    text = re.sub(r"\s+", " ", text).strip()
    
    return text


# =============================================
# CLEAN CONTENT
# Mục đích: Làm sạch nội dung HTML và chuẩn hóa văn bản
# =============================================
def clean_content(raw: str) -> str:
    """
    Làm sạch nội dung HTML và chuẩn hóa text
    Input: raw - Chuỗi HTML thô
    Output: Chuỗi văn bản đã được làm sạch
    """
    # Kiểm tra input rỗng
    if not raw:
        return ""
    text = raw
    
    # Loại bỏ các nội dung blacklist (Video Player, Modal window)
    blacklist_patterns = [
        r"Video Player.*?End of dialog window",
        r"This is a modal window.*?End of dialog window"
    ]
    for pattern in blacklist_patterns:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE | re.DOTALL)

    # Parse HTML bằng BeautifulSoup
    soup = BeautifulSoup(text, "html.parser")
    
    # Loại bỏ các thẻ HTML không cần thiết
    for tag in soup(["script", "style", "iframe", "video", "source", "button", "noscript", "meta", "link"]):
        tag.decompose()

    # Trích xuất text từ HTML
    clean = soup.get_text(" ", strip=True)

    # ================= CHUẨN HÓA KHOẢNG TRẮNG & DẤU CÂU =================
    # Chuẩn hóa nhiều khoảng trắng thành 1
    clean = re.sub(r"\s+", " ", clean)
    
    # Xóa ký tự thừa đầu đoạn
    clean = re.sub(r"^[\s\.:;,-]+", "", clean)
    
    # Xóa ký tự thừa cuối đoạn
    clean = re.sub(r"[\s\.:;,-]+$", "", clean)
    
    # Trim khoảng trắng đầu cuối
    clean = clean.strip()
    
    # Thêm dấu chấm cuối câu nếu chưa có
    if clean and clean[-1] not in ".?!":
        clean += "."

    # Áp dụng xử lý tách từ tiếng Việt
    clean = advanced_vietnamese_spacing(clean)
    
    return clean


# =============================================
# TRANSFORM LOADER KÈM LOGGING
# Mục đích: Transform dữ liệu từ staging sang production và ghi log
# =============================================
class TransformLoader:
    """
    Class quản lý quá trình Transform dữ liệu
    """
    def __init__(self, db="news_staging_db", job_name="Transform_Staging"):
        """
        Khởi tạo TransformLoader
        """
        # 1: Kết nối database
        self.conn = connect_to_db(db)
        
        # 2: Tạo cursor
        self.cursor = self.conn.cursor(dictionary=True)
        self.job_name = job_name
        
        # 3: Log START và lấy run_id
        self.run_id, _ = log_start(job_name)
        print(f"[INFO] RUN_ID: {self.run_id}")

    def build_clean_staging(self):
        """
        Xây dựng bảng staging_clean_table từ staging_temp_table
        """
        # 4: Tạo bảng staging_clean_table
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging_clean_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                article_url VARCHAR(1000),
                source_name VARCHAR(255),
                category VARCHAR(255),
                author VARCHAR(1000),
                published_at VARCHAR(255),
                title VARCHAR(1000),
                summary TEXT,
                content TEXT,
                tags TEXT,
                scraped_at VARCHAR(255)
            )
        """)
        
        # 5: Truncate bảng để xóa dữ liệu cũ
        self.cursor.execute("TRUNCATE TABLE staging_clean_table")

        # 6: Lấy tất cả dữ liệu từ staging_temp_table
        self.cursor.execute("SELECT * FROM staging_temp_table")
        rows = self.cursor.fetchall()
        
        count = 0
        # 7: Lặp qua từng record để clean và insert
        for row in rows:
            # 7.1: Clean content
            cleaned_content = clean_content(row.get("content", ""))
            
            # 7.2: Insert vào staging_clean_table
            self.cursor.execute("""
                INSERT INTO staging_clean_table
                (article_url, source_name, category, author, published_at,
                 title, summary, content, tags, scraped_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                row.get("article_url"), row.get("source_name"), row.get("category"),
                row.get("author"), row.get("published_at"), row.get("title"),
                row.get("summary"), cleaned_content, row.get("tags"), row.get("scraped_at")
            ))
            count += 1
        
        # 8: Commit transaction
        self.conn.commit()
        print(f"[INFO] Cleaned {count} records.")

    def run_transform(self):
        """
        Chạy quá trình Transform chính
        """
        total_raw = total_success = total_failed = 0
        try:
            # 9: Build clean staging table
            self.build_clean_staging()
            
            # 10: Gọi stored procedure transform
            self.cursor.callproc("sp_transform_news_data", [self.run_id])
            self.conn.commit()
            
            # 11: Đếm số record transform thành công
            self.cursor.execute("SELECT COUNT(*) AS cnt FROM transformed_temp_table WHERE run_id=%s", (self.run_id,))
            total_success = self.cursor.fetchone()["cnt"]
            total_raw = total_success
            
            # 12: Log END với status SUCCESS
            log_end(self.run_id, "SUCCESS", total_raw, total_success)
            print(f"[OK] Transform hoàn tất, RUN_ID: {self.run_id}")
            
        except Exception as e:
            # 12.1: Log END với status FAILED
            total_failed = 1
            log_end(self.run_id, "FAILED", total_raw, total_success, str(e))
            print(f"[ERROR] Transform failed: {e}")
            self.conn.rollback()

    def close(self):
        """
        Đóng kết nối database
        """
        # 13: Đóng cursor
        self.cursor.close()
        
        # 14: Đóng connection
        self.conn.close()


if __name__ == "__main__":
    # Khởi tạo TransformLoader
    loader = TransformLoader()
    try:
        #  Chạy quá trình Transform
        loader.run_transform()
    finally:
        #  Đóng kết nối (luôn chạy dù có lỗi hay không)
        loader.close()
    
    #  In thông báo hoàn tất
    print("ETL JOB FINISHED!")
