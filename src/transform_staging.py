from utils.db_utils import connect_to_db
from bs4 import BeautifulSoup
import re
import uuid

# =====================================================================
# STEP X — FIX WORD SPACING (GENERAL VIETNAMESE WORD SPLITTER)
# =====================================================================
from pyvi import ViTokenizer

def fix_word_spacing(text: str) -> str:
    """
    Tách từ tiếng Việt tổng quát cho mọi dữ liệu.
    Không tách chữ hoa, số, dấu câu bừa bãi.
    """
    if not text:
        return ""
    
    text = ViTokenizer.tokenize(text)
    text = text.replace("_", " ")
    text = " ".join(text.split())  # Chuẩn hóa khoảng trắng
    return text


# =====================================================================
# CLEAN STEP — CLEAN HTML + REMOVE UI + ADS
# =====================================================================
def clean_content(raw):
    if not raw:
        return ""

    text = raw

    # =========================================================
    # STEP 1 — REMOVE IRRELEVANT TEXT USING REGEX
    # =========================================================
    blacklist = [
        r"Video:\s*[^\n]+",
        r"Ảnh:\s*[^\n]+",
        r"Video\s*\:[^\n]+",
        r"Ảnh\s*\:[^\n]+",
        r"Chia sẻ\s*\:\s*Facebook\s*Twitter",
        r"Sao chép liên kết",
        r"Sao chép",
        r"Copy link thành công",
        r"Copy link",
        r"Copy",
        r"Click để xem.*",
        r"Nhấn để xem.*",
        r"Quỹ Hy Vọng.*?(Hotline|Duy Phong|Chi nhánh|Swift code|Số tài khoản).*",
        r"Vui lòng.*?chuyển khoản.*",
        r"Chuyển khoản ngân hàng.*",
        r"Swift code\/BIC.*",
        r"Văn phòng Quỹ Hy vọng.*",
        r"Video Player.*?End of dialog window",
        r"This is a modal window.*?End of dialog window",
        r"Quảng cáo.*",
        r"Created with Highcharts.*",
        r"Xem.*?tại đây.*",
        r"[Xx]em thêm.*",
        r"Đọc thêm.*",
        r"Tham khảo thêm.*",
        r"[0-9]{1,3}\s*biểu quyết",
        r"Ảnh minh hoạ.*",
        r"Đồ họa.*",
        r"Infographic.*",
    ]

    for p in blacklist:
        text = re.sub(p, " ", text, flags=re.DOTALL)

    # =========================================================
    # STEP 2 — REMOVE UNNEEDED HTML TAGS
    # =========================================================
    soup = BeautifulSoup(text, "html.parser")
    for tag in soup(["script", "style", "iframe", "video", "source", "button"]):
        tag.decompose()

    # =========================================================
    # STEP 3 — REMOVE UI DIV BLOCKS
    # =========================================================
    bad_classes = [
        "modal", "advertisement", "ads", "banner", "player",
        "vne-video", "vne-related", "social-share", "share",
        "tool-box", "toolbar", "interaction", "copyright"
    ]
    for cl in bad_classes:
        for div in soup.find_all(class_=cl):
            div.decompose()

    # =========================================================
    # STEP 4 — EXTRACT TEXT FROM CLEAN HTML
    # =========================================================
    clean = soup.get_text(" ", strip=True)

    # =========================================================
    # STEP 5 — REMOVE LEFTOVER MEDIA CAPTIONS
    # =========================================================
    clean = re.sub(r"(Ảnh|Video)\s*:[^\n]+", " ", clean)
    clean = re.sub(r"(Ảnh|Video)\s*\w.*", " ", clean)

    # =========================================================
    # STEP 6 — REMOVE ACTION PROMPTS
    # =========================================================
    clean = re.sub(r"(Đăng ký nhận tin|Đăng ký tư vấn|Liên hệ quảng cáo).*", " ", clean)

    # =========================================================
    # STEP 7 — NORMALIZE WHITESPACES
    # =========================================================
    clean = re.sub(r"\s+", " ", clean).strip()

    # =========================================================
    # STEP 7.5 — FINAL TEXT FIXES (REMOVE LEADING/TRAILING NOISE + ADD PERIOD)
    # =========================================================
    # Bỏ ký tự vô nghĩa ở đầu đoạn
    clean = re.sub(r"^[\.\s:;'\-]+", "", clean)

    # Bỏ ký tự dư cuối đoạn
    clean = re.sub(r"[\s:;]+$", "", clean)

    # Thêm dấu chấm kết thúc nếu chưa có
    if clean and not clean.endswith("."):
        clean += "."

    # =========================================================
    # STEP 8 — FIX WORD SPACING (TOKENIZER TIẾNG VIỆT)
    # =========================================================
    clean = fix_word_spacing(clean)

    return clean


# =====================================================================
# TRANSFORM LOADER — ETL MAIN PIPELINE
# =====================================================================
class TransformLoader:
    def __init__(self, db="news_staging_db"):
        self.conn = connect_to_db(db)
        self.cursor = self.conn.cursor(dictionary=True)
        self.run_id = str(uuid.uuid4())
        print(f"Kết nối {db} thành công!")

    def build_clean_staging(self):
        print("[INFO] Creating and building staging_clean_table...")

        # =========================================================
        # Tạo bảng nếu chưa tồn tại
        # =========================================================
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
            );
        """)

        # =========================================================
        # Xóa dữ liệu cũ
        # =========================================================
        self.cursor.execute("TRUNCATE staging_clean_table")
        print("Đã xóa toàn bộ dữ liệu cũ trong staging_clean_table.")

        # =========================================================
        # Lấy dữ liệu từ staging_temp_table
        # =========================================================
        self.cursor.execute("SELECT * FROM staging_temp_table")
        rows = self.cursor.fetchall()
        print(f"Số bản ghi ban đầu trong staging_temp_table: {len(rows)}")

        # =========================================================
        # Load vào staging_clean_table sau khi clean
        # =========================================================
        count = 0
        for row in rows:
            cleaned = clean_content(row["content"])
            self.cursor.execute("""
                INSERT INTO staging_clean_table 
                (article_url, source_name, category, author, published_at, 
                title, summary, content, tags, scraped_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, (
                row["article_url"],
                row["source_name"],
                row["category"],
                row["author"],
                row["published_at"],
                row["title"],
                row["summary"],
                cleaned,
                row["tags"],
                row["scraped_at"]
            ))
            count += 1

        self.conn.commit()
        print(f"Đã nạp {count} bản ghi vào staging_clean_table với run_id {self.run_id}.")

    def run_transform(self):
        try:
            self.build_clean_staging()
            print("[INFO] Calling stored procedure sp_transform_news_data...")
            self.cursor.callproc("sp_transform_news_data", [self.run_id])
            self.conn.commit()

            # =========================================================
            # Đếm số bản ghi insert vào transformed_temp_table
            # =========================================================
            self.cursor.execute("SELECT COUNT(*) AS cnt FROM transformed_temp_table WHERE run_id=%s", (self.run_id,))
            inserted_count = self.cursor.fetchone()["cnt"]
            print(f"Số bản ghi insert vào transformed_temp_table: {inserted_count}")

            print(f"[OK] Transform completed — run_id = {self.run_id}")
            print("[INFO] Cleaning staging_clean_table...")
            self.cursor.execute("DROP TABLE IF EXISTS staging_clean_table")
            self.conn.commit()
            print("[OK] staging_clean_table has been DROPPED.")

        except Exception as e:
            print(f"[ERROR] Transform failed: {e}")
            self.conn.rollback()

    def close(self):
        self.cursor.close()
        self.conn.close()


# =====================================================================
# ENTRY POINT — START ETL JOB
# =====================================================================
if __name__ == "__main__":
    loader = TransformLoader()
    try:
        loader.run_transform()
    finally:
        loader.close()
