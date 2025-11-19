import re
import json
from datetime import datetime, date
from utils.db_utils import connect_to_db

# ====== Mapping nguồn & chuyên mục ======
SOURCE_MAP = {
    "vnexpress": "VnExpress",
    "tuoitre": "Tuổi Trẻ"
}

CATEGORY_MAP = {
    "xa hoi": "Xã hội",
    "thoi su": "Thời sự",
    "the gioi": "Thế giới",
    "kinh doanh": "Kinh doanh",
    "giao duc": "Giáo dục",
    "phap luat": "Pháp luật",
    "giai tri": "Giải trí",
    "the thao": "Thể thao",
    "suc khoe": "Sức khỏe",
    "cong nghe": "Công nghệ",
    "doi song": "Đời sống",
    "du lich": "Du lịch",
    "khoa hoc": "Khoa học",
    "oto xe may": "Ôtô - Xe máy",
    "bat dong san": "Bất động sản",
    "tam ly": "Tâm lý"
}

POSITIVE_WORDS = [
    "tốt", "vui", "hạnh phúc", "thành công", "xuất sắc", "ấn tượng", "hài lòng",
    "khởi sắc", "khỏe mạnh", "phát triển", "đột phá", "giải thưởng", "thăng tiến",
    "lợi ích", "sáng tạo", "đáng chú ý", "thuận lợi", "tăng trưởng", "tiến bộ",
    "hưng thịnh", "thành tựu", "khích lệ", "khả quan", "lạc quan", "phát đạt",
    "an toàn", "bình an", "hài hòa", "đầu tư", "thu nhập", "khen thưởng",
    "bứt phá", "cải thiện", "thịnh vượng", "lợi nhuận", "tiềm năng", "giải pháp",
    "khởi nghiệp", "cơ hội", "hài hước", "trẻ trung", "nổi bật", "hấp dẫn"
]

NEGATIVE_WORDS = [
    "xấu", "tệ", "buồn", "thất bại", "khó", "nguy hiểm", "tai nạn", "thiệt hại",
    "khủng hoảng", "bạo lực", "tàn phá", "sụp đổ", "gián đoạn", "chậm trễ", "lừa đảo",
    "phá sản", "lỗ", "biến động", "căng thẳng", "khó khăn", "bất lợi", "suy thoái",
    "mất mát", "bạo loạn", "xung đột", "đe dọa", "thảm họa", "thiên tai", "ôn dịch",
    "tội phạm", "khủng bố", "nguy cơ", "phản đối", "tranh cãi",
    "tiêu cực", "bất ổn", "suy giảm", "đổ vỡ", "gây tổn hại", "khủng hoảng kinh tế",
    "mất việc", "tắc nghẽn", "đe dọa an ninh", "phản cảm", "khủng hoảng xã hội"
]

class TransformLoader:
    def __init__(self, db_name="news_staging_db"):
        self.conn = connect_to_db(db_name)
        if not self.conn:
            raise ConnectionError("Không thể kết nối database.")
        self.cursor = self.conn.cursor(dictionary=True)

    # -------------------------
    # Fetch staging
    # -------------------------
    def fetch_staging_data(self):
        self.cursor.execute("SELECT * FROM staging_temp_table")
        return self.cursor.fetchall()

    # -------------------------
    # Helpers
    # -------------------------
    @staticmethod
    def is_empty_value(val):
        if val is None:
            return True
        if isinstance(val, str):
            return val.strip().lower() in ["", "n/a", "na", "null"]
        return False

    @staticmethod
    def normalize_author(name):
        if not name:
            return ""
        name = name.replace("(tổng hợp)", "").strip().lower()
        name = re.sub(r"[^a-zA-ZÀ-ỹ\s]", "", name)
        return " ".join(w.capitalize() for w in name.split())

    @staticmethod
    def normalize_tags(raw_tags):
        if not raw_tags:
            return json.dumps([], ensure_ascii=False)
        tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
        tags = list(dict.fromkeys(tags))
        return json.dumps(tags, ensure_ascii=False)

    @staticmethod
    def parse_published_at(raw):
        if not raw:
            return None
        s = raw.strip()
        s = re.sub(r"^[^\d]+,\s*", "", s)
        s = s.replace("GMT+7", "+0700").replace("(", "").replace(")", "")
        fmts = ["%d/%m/%Y, %H:%M %z","%d/%m/%Y, %H:%M","%d/%m/%Y %H:%M","%d/%m/%Y"]
        for f in fmts:
            try:
                return datetime.strptime(s, f)
            except:
                pass
        return None

    @staticmethod
    def calc_sentiment(text):
        if not text:
            return 0.0
        text_lower = text.lower()
        score = 0
        for w in POSITIVE_WORDS:
            if w in text_lower:
                score += 1
        for w in NEGATIVE_WORDS:
            if w in text_lower:
                score -= 1
        return float(score)

    @staticmethod
    def clean_text(text):
        if not text:
            return ""
        return re.sub(r"<[^>]*>", "", text).strip()

    # -------------------------
    # Transform row
    # -------------------------
    def transform_row(self, row):
        if not row.get("article_url"):
            return None

        # Bỏ các dòng >2 cột trống/N/A/null
        empty_count = sum(
            self.is_empty_value(row.get(col))
            for col in ["source_name_raw", "category_raw", "author_raw",
                        "title_raw", "summary_raw", "content_raw", "tags_raw", "published_at_raw"]
        )
        if empty_count > 2:
            return None

        source = SOURCE_MAP.get((row.get("source_name_raw") or "").lower(), row.get("source_name_raw") or "")
        category = CATEGORY_MAP.get((row.get("category_raw") or "").lower(), row.get("category_raw") or "")
        author = self.normalize_author(row.get("author_raw") or "")
        title = self.clean_text(row.get("title_raw"))
        description = self.clean_text(row.get("summary_raw"))
        content = row.get("content_raw") or ""
        word_count = len(content.split())
        tags = self.normalize_tags(row.get("tags_raw"))
        published_at = self.parse_published_at(row.get("published_at_raw"))
        date_dim = int(published_at.strftime("%Y%m%d")) if published_at else int(date.today().strftime("%Y%m%d"))
        sentiment_score = self.calc_sentiment(content)

        return (
            row.get("article_url"), source, category, author,
            published_at, title, description, word_count, tags,
            sentiment_score, row.get("run_id"), date_dim
        )

    # -------------------------
    # Xóa bảng cũ
    # -------------------------
    def truncate_table(self):
        self.cursor.execute("TRUNCATE TABLE transformed_temp_table")
        self.conn.commit()
        print("Truncated transformed_temp_table")

    # -------------------------
    # Batch insert
    # -------------------------
    def batch_insert(self, transformed_rows):
        sp_call = """
            CALL sp_insert_transformed_batch(
                %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s
            )
        """
        self.cursor.executemany(sp_call, transformed_rows)
        self.conn.commit()
        print(f"Inserted {len(transformed_rows)} rows into transformed_temp_table")

    # -------------------------
    # ETL
    # -------------------------
    def run_etl(self):
        data = self.fetch_staging_data()
        transformed_rows = [self.transform_row(r) for r in data if self.transform_row(r)]
        print(f"Total rows in staging: {len(data)}")
        print(f"Rows after filtering empty columns: {len(transformed_rows)}")
        if transformed_rows:
            self.truncate_table()
            self.batch_insert(transformed_rows)
        else:
            print("No valid data to insert.")

    def close(self):
        self.cursor.close()
        self.conn.close()

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    loader = TransformLoader()
    try:
        loader.run_etl()
    finally:
        loader.close()
