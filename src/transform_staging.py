from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end
from bs4 import BeautifulSoup
import re
import uuid
from pyvi import ViTokenizer


# =============================================
# SIÊU HÀM FIX DÍNH TỪ TIẾNG VIỆT
# =============================================
def advanced_vietnamese_spacing(text: str) -> str:
    if not text or not text.strip():
        return ""
    text = text.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')
    text = ViTokenizer.tokenize(text).replace("_", " ")
    # Xử lý các trường hợp dính từ nặng
    text = re.sub(r"([a-zàáạảãâăêôơưéèẻẽẹđỳỳỷỹ])([A-ZÀÁẠẢÃÂĂÊÔƠƯÉÈẺẼẸĐỲỶỸ])", r"\1 \2", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


# =============================================
# CLEAN CONTENT
# =============================================
def clean_content(raw: str) -> str:
    if not raw:
        return ""
    text = raw
    # Regex blacklist + remove HTML tags
    blacklist_patterns = [
        r"Video Player.*?End of dialog window",
        r"This is a modal window.*?End of dialog window"
    ]
    for pattern in blacklist_patterns:
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE | re.DOTALL)

    soup = BeautifulSoup(text, "html.parser")
    for tag in soup(["script", "style", "iframe", "video", "source", "button", "noscript", "meta", "link"]):
        tag.decompose()

    clean = soup.get_text(" ", strip=True)

    # ================= CHUẨN HÓA KHOẢNG TRẮNG & DẤU CÂU =================
    clean = re.sub(r"\s+", " ", clean)
    clean = re.sub(r"^[\s\.:;,-]+", "", clean)   # xóa ký tự thừa đầu đoạn
    clean = re.sub(r"[\s\.:;,-]+$", "", clean)   # xóa ký tự thừa cuối đoạn
    clean = clean.strip()
    if clean and clean[-1] not in ".?!":
        clean += "."

    clean = advanced_vietnamese_spacing(clean)
    return clean


# =============================================
# TRANSFORM LOADER KÈM LOGGING
# =============================================
class TransformLoader:
    def __init__(self, db="news_staging_db", job_name="Transform_Staging"):
        self.conn = connect_to_db(db)
        self.cursor = self.conn.cursor(dictionary=True)
        self.job_name = job_name
        # Log START
        self.run_id, _ = log_start(job_name)
        print(f"[INFO] RUN_ID: {self.run_id}")

    def build_clean_staging(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS staging_clean_table (
                id INT AUTO_INCREMENT PRIMARY KEY,
                article_url VARCHAR(1000),
                source_name VARCHAR(255),
                category VARCHAR(255),
                author VARCHAR(255),
                published_at VARCHAR(255),
                title VARCHAR(1000),
                summary TEXT,
                content TEXT,
                tags TEXT,
                scraped_at VARCHAR(255)
            )
        """)
        self.cursor.execute("TRUNCATE TABLE staging_clean_table")

        self.cursor.execute("SELECT * FROM staging_temp_table")
        rows = self.cursor.fetchall()
        count = 0
        for row in rows:
            cleaned_content = clean_content(row.get("content", ""))
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
        self.conn.commit()
        print(f"[INFO] Cleaned {count} records.")

    def run_transform(self):
        total_raw = total_success = total_failed = 0
        try:
            self.build_clean_staging()
            self.cursor.callproc("sp_transform_news_data", [self.run_id])
            self.conn.commit()
            self.cursor.execute("SELECT COUNT(*) AS cnt FROM transformed_temp_table WHERE run_id=%s", (self.run_id,))
            total_success = self.cursor.fetchone()["cnt"]
            total_raw = total_success
            log_end(self.run_id, "SUCCESS", total_raw, total_success)
            print(f"[OK] Transform hoàn tất, RUN_ID: {self.run_id}")
        except Exception as e:
            total_failed = 1
            log_end(self.run_id, "FAILED", total_raw, total_success, str(e))
            print(f"[ERROR] Transform failed: {e}")
            self.conn.rollback()

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    loader = TransformLoader()
    try:
        loader.run_transform()
    finally:
        loader.close()
    print("ETL JOB FINISHED!")
