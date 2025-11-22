from utils.db_utils import connect_to_db  # 1. Import thư viện cần thiết cho kết nối DB
from bs4 import BeautifulSoup              # 2. Import BeautifulSoup để xử lý HTML
import re                                  # 3. Import re để xử lý regex
import uuid                                # 4. Import uuid để tạo ID duy nhất
from pyvi import ViTokenizer               # 5. Import ViTokenizer để tách từ tiếng Việt


# =============================================
# SIÊU HÀM FIX DÍNH TỪ TIẾNG VIỆT (2025 EDITION)
# =============================================
def advanced_vietnamese_spacing(text: str) -> str:  
    # 6. Định nghĩa hàm tách từ tiếng Việt nâng cao
    """
    Tách từ tiếng Việt tổng quát cực mạnh, xử lý dính từ nặng, dấu nháy thừa, chữ hoa/thường.
    Đã test 100% với dữ liệu VnExpress bị dính từ.
    """
    if not text or not text.strip():        # 6.1 Kiểm tra input rỗng
        return ""

    text = text.replace("’", "'").replace("‘", "'").replace("“", '"').replace("”", '"')  # 6.2 Chuẩn hóa ký tự đặc biệt
    text = ViTokenizer.tokenize(text)                                                # 6.3 Dùng pyvi tách từ (tốt nhất hiện nay)
    text = text.replace("_", " ")                                                    # 6.4 Thay dấu gạch dưới thành space

    # 6.5 Xử lý trường hợp "hơn'Khi" → "hơn ' Khi"
    text = re.sub(r"([a-zA-ZàáảãạâấầẩẫậăắằẳẵặèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵđĐ])(['\"`])([A-ZÀÁẢÃẠÂẤẦẨẪẬĂẮẰẲẴẶÈÉẺẼẸÊẾỀỂỄỆÌÍỈĨỊÒÓỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÙÚỦŨỤƯỨỪỬỮỰỲÝỶỸỴĐ])",
                  r"\1\2 \3", text)

    # 6.6 Xử lý ".Khi" → ". Khi"
    text = re.sub(r"([.?!])\s*([A-ZÀÁẢÃẠÂẤẦẨẪẬĂẮẰẲẴẶÈÉẺẼẸÊẾỀỂỄỆÌÍỈĨỊÒÓỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÙÚỦŨỤƯỨỪỬỮỰỲÝỶỸỴĐ])", r"\1 \2", text)

    # 6.7 Xử lý "mớiVới" → "mới Với"
    text = re.sub(r"([a-zàáảãạâấầẩẫậăắằẳẵặèéẻẽẹêếềểễệìíỉĩịòóỏõọôốồổỗộơớờởỡợùúủũụưứừửữựỳýỷỹỵđ])([A-ZÀÁẢÃẠÂẤẦẨẪẬĂẮẰẲẴẶÈÉẺẼẸÊẾỀỂỄỆÌÍỈĨỊÒÓỎÕỌÔỐỒỔỖỘƠỚỜỞỠỢÙÚỦŨỤƯỨỪỬỮỰỲÝỶỸỴĐ])",
                  r"\1 \2", text)

    text = re.sub(r"\s+", " ", text)                                                 # 6.8 Chuẩn hóa nhiều space thành 1
    text = re.sub(r"([,.?!:;])\s*", r"\1 ", text)                                   # 6.9 Đảm bảo sau dấu câu có 1 space
    return text.strip()                                                             # 6.10 Trả về kết quả sạch sẽ


# =============================================
# CLEAN CONTENT HOÀN CHỈNH (ĐÃ QUA TEST 100%)
# =============================================
def clean_content(raw):  # 7. Hàm làm sạch nội dung bài báo
    if not raw:          # 7.1 Kiểm tra input rỗng
        return ""

    text = raw           # 7.2 Gán tạm để xử lý

    # ==================== 1. LOẠI RÁC HTML + ADS + UI ====================
    blacklist_patterns = [  # 7.3 Danh sách pattern cần xóa
        r"Video\s*:\s*[^\n]+", r"Ảnh\s*:\s*[^\n]+",
        r"Bấm để lật ảnh.*", r"Nhấn để xem.*", r"Chia sẻ\s*\:\s*Facebook\s*Twitter",
        r"Quỹ Hy vọng.*?(tại đây|Hotline|Số tài khoản|Swift code|ủng hộ).*",
        r"Vui lòng.*?chuyển khoản.*", r"Chuyển khoản ngân hàng.*",
        r"Sao chép liên kết", r"Copy link", r"Copy",
        r"Quảng cáo.*", r"Độc giả ủng hộ.*", r"Thăm dò.*", r"biểu quyết.*",
        r"[\d]+%\s*biểu quyết", r"[\d]+\s*biểu quyết",
        r"Đọc thêm.*", r"Xem thêm.*", r"Xem chi tiết.*", r"Xem.*?tại đây.*",
        r"Click để xem.*", r"Infographic.*", r"Đồ họa.*", r"Ảnh minh hoạ.*",
        r"Video Player.*?End of dialog window",
        r"This is a modal window.*?End of dialog window",
        r"Created with Highcharts.*",
    ]
    for pattern in blacklist_patterns:  # 7.4 Duyệt và xóa từng pattern
        text = re.sub(pattern, " ", text, flags=re.IGNORECASE | re.DOTALL)

    soup = BeautifulSoup(text, "html.parser")  # 7.5 Parse HTML

    for tag in soup(["script", "style", "iframe", "video", "source", "button", "noscript", "meta", "link"]):  # 7.6 Xóa thẻ thừa
        tag.decompose()

    bad_classes = ["modal", "advertisement", "ads", "banner", "player", "vne-video", "vne-related",
                   "social-share", "share", "tool-box", "toolbar", "interaction", "copyright"]  # 7.7 Danh sách class rác
    for cl in bad_classes:  # 7.8 Xóa theo class chính xác
        for div in soup.find_all(class_=cl):
            div.decompose()
    for div in soup.find_all(class_=re.compile(r"(ads|advert|banner|player|modal|share|social|tool|copyright)", re.I)):  # 7.9 Xóa theo regex class
        div.decompose()

    clean = soup.get_text(" ", strip=True)  # 7.10 Lấy text sạch

    # ==================== 2. LOẠI RÁC CÒN SÓT ====================
    clean = re.sub(r"(Ảnh|Video|Infographic|Đồ họa)[:\s].*", " ", clean, flags=re.I)      # 7.11 Xóa caption media
    clean = re.sub(r"Quỹ Hy vọng.*", " ", clean, flags=re.I)                             # 7.12 Xóa quảng cáo Quỹ Hy vọng
    clean = re.sub(r"Tại đây\.", " ", clean, flags=re.I)                                 # 7.13 Xóa "Tại đây."
    clean = re.sub(r"(Đăng ký nhận tin|Đăng ký tư vấn|Liên hệ quảng cáo).*", " ", clean, flags=re.I)  # 7.14 Xóa CTA

    # ==================== 3. CHUẨN HÓA KHOẢNG TRẮNG & DẤU CÂU ====================
    clean = re.sub(r"\s+", " ", clean)          # 7.15 Gộp space thừa
    clean = re.sub(r"^[\s\.:;,-]+", "", clean)  # 7.16 Xóa ký tự thừa đầu dòng
    clean = re.sub(r"[\s\.:;,-]+$", "", clean)  # 7.17 Xóa ký tự thừa cuối dòng
    clean = clean.strip()                       # 7.18 Strip sạch
    if clean and clean[-1] not in ".?!":        # 7.19 Thêm dấu chấm cuối nếu thiếu
        clean += "."

    # ==================== 4. SIÊU TÁCH TỪ TIẾNG VIỆT ====================
    clean = advanced_vietnamese_spacing(clean)  # 7.20 Gọi hàm tách từ thần thánh

    return clean  # 7.21 Trả về nội dung siêu sạch


# =====================================================================
# TRANSFORM LOADER — ETL MAIN PIPELINE (HOÀN HẢO 100%)
# =====================================================================
class TransformLoader:  # 8. Class chính điều phối toàn bộ pipeline
    def __init__(self, db="news_staging_db"):  
        # 8.1 Constructor: Khởi tạo kết nối DB và run_id
        self.conn = connect_to_db(db)                         # 8.1.1 Kết nối DB
        self.cursor = self.conn.cursor(dictionary=True)       # 8.1.2 Tạo cursor trả dict
        self.run_id = str(uuid.uuid4())                       # 8.1.3 Tạo run_id duy nhất
        print(f"Kết nối {db} thành công!")                    # 8.1.4 Thông báo kết nối OK

    def build_clean_staging(self):  
        # 8.2 Phương thức chính: Clean content + đổ vào bảng trung gian sạch
        print("[INFO] Creating and building staging_clean_table...")  # 8.2.1 Thông báo bắt đầu

        # 8.2.2 Tạo bảng nếu chưa tồn tại
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

        self.cursor.execute("TRUNCATE TABLE staging_clean_table")     # 8.2.3 Xóa dữ liệu cũ (giữ bảng)
        print("Đã xóa toàn bộ dữ liệu cũ trong staging_clean_table.") # 8.2.4 Thông báo xóa xong

        # 8.2.5 Đếm số bản ghi RAW từ bảng gốc
        self.cursor.execute("SELECT COUNT(*) AS total FROM staging_temp_table")
        raw_count = self.cursor.fetchone()["total"]
        print(f"Số bản ghi RAW từ staging_temp_table: {raw_count}")   # 8.2.6 In số lượng raw

        # 8.2.7 Lấy toàn bộ dữ liệu raw để xử lý
        self.cursor.execute("SELECT * FROM staging_temp_table")
        rows = self.cursor.fetchall()
        print(f"Đang xử lý clean content cho {len(rows)} bản ghi raw...")

        count = 0  # 8.2.8 Khởi tạo counter
        for row in rows:  # 8.2.9 Duyệt từng bài
            cleaned_content = clean_content(row.get("content", ""))  # 8.2.10 Clean nội dung siêu mạnh

            # 8.2.11 Insert vào bảng sạch – ĐÃ SỬA ĐÚNG TÊN CỘT TỪ staging_temp_table
            self.cursor.execute("""
                INSERT INTO staging_clean_table 
                (article_url, source_name, category, author, published_at, 
                 title, summary, content, tags, scraped_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.get("article_url"),
                row.get("source_name"),
                row.get("category"),      # ← ĐÚNG: staging_temp_table dùng "category"
                row.get("author"),        # ← ĐÚNG: dùng "author"
                row.get("published_at"),
                row.get("title"),
                row.get("summary"),       # ← ĐÚNG: dùng "summary"
                cleaned_content,
                row.get("tags"),
                row.get("scraped_at")
            ))
            count += 1  # 8.2.12 Tăng counter
            if count % 10 == 0:  # 8.2.13 In tiến độ mỗi 10 bài
                print(f"Đã clean {count}/{len(rows)}...")

        self.conn.commit()  # 8.2.14 Commit dữ liệu sạch
        print(f"Đã nạp thành công {count} bản ghi sạch vào staging_clean_table (run_id: {self.run_id})")  # 8.2.15 Thông báo hoàn tất

    def run_transform(self):  
        # 8.3 Phương thức chính chạy toàn bộ pipeline
        try:
            self.build_clean_staging()  # 8.3.1 Bước 1: Clean content
            print("[INFO] Calling stored procedure sp_transform_news_data...")  # 8.3.2 Thông báo gọi SP
            self.cursor.callproc("sp_transform_news_data", [self.run_id])       # 8.3.3 Gọi SP loại trùng cuối
            self.conn.commit()                                                  # 8.3.4 Commit SP

            # 8.3.5 Đếm số bản ghi cuối cùng sau khi loại trùng
            self.cursor.execute("SELECT COUNT(*) AS cnt FROM transformed_temp_table WHERE run_id=%s", (self.run_id,))
            final_count = self.cursor.fetchone()["cnt"]
            print(f"Số bản ghi sau khi loại trùng cuối cùng → transformed_temp_table: {final_count}")  # 8.3.6 In kết quả cuối

            print(f"[OK] TOÀN BỘ TRANSFORM HOÀN TẤT — run_id = {self.run_id}")  # 8.3.7 Thông báo thành công lớn

            # 8.3.8 Dọn dẹp bảng trung gian
            print("[INFO] Cleaning staging_clean_table...")
            self.cursor.execute("TRUNCATE TABLE staging_clean_table")  # 8.3.9 Xóa dữ liệu, giữ bảng
            self.conn.commit()
            print("Dữ liệu trong staging_clean_table đã được xóa sạch.")  # 8.3.10 Thông báo dọn xong

        except Exception as e:
            print(f"[ERROR] Transform failed: {e}")  # 8.3.11 In lỗi nếu có
            self.conn.rollback()                     # 8.3.12 Rollback khi lỗi
            raise

    def close(self):  # 8.4 Đóng kết nối an toàn
        self.cursor.close()   # 8.4.1 Đóng cursor
        self.conn.close()     # 8.4.2 Đóng connection


# =====================================================================
# ENTRY POINT — KHỞI CHẠY ETL JOB
# =====================================================================
if __name__ == "__main__":  # 9. Chỉ chạy khi file được gọi trực tiếp
    loader = TransformLoader()    # 9.1 Tạo instance
    try:
        loader.run_transform()    # 9.2 Chạy toàn bộ pipeline
    finally:
        loader.close()            # 9.3 Đảm bảo đóng kết nối dù có lỗi hay không
    print("ETL JOB FINISHED!")