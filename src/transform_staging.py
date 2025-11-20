import re
from datetime import datetime
from utils.db_utils import connect_to_db
import json

# ============================
# Mapping
# ============================

SOURCE_MAP = {
    "vnexpress": "VnExpress",
    "tuoitre": "Tuổi Trẻ"
}

CATEGORY_MAP = {
    "xa hoi": "Xã hội", "thoi su": "Thời sự", "the gioi": "Thế giới",
    "kinh doanh": "Kinh doanh", "giao duc": "Giáo dục", "phap luat": "Pháp luật",
    "giai tri": "Giải trí", "the thao": "Thể thao", "suc khoe": "Sức khỏe",
    "cong nghe": "Công nghệ", "doi song": "Đời sống", "du lich": "Du lịch",
    "khoa hoc": "Khoa học", "oto xe may": "Ôtô - Xe máy",
    "bat dong san": "Bất động sản", "tam ly": "Tâm lý"
}

# ============================
# Loader
# ============================

class TransformLoader:
    def __init__(self, db_name="news_staging_db"):
        self.conn = connect_to_db(db_name)
        if not self.conn:
            raise ConnectionError("Không thể kết nối DB")
        self.cursor = self.conn.cursor(dictionary=True)

    # ===== Fetch =====
    def fetch_staging_data(self):
        self.cursor.execute("SELECT * FROM staging_temp_table")
        return self.cursor.fetchall()

    # ===== Helpers =====
    @staticmethod
    def is_empty(val):
        if val is None:
            return True
        if isinstance(val, str):
            return val.strip().lower() in ["", "null", "n/a", "na"]
        return False

    @staticmethod
    def clean_html(text):
        if not text:
            return ""
        return re.sub(r"<[^>]*>", "", text).strip()

    @staticmethod
    def normalize_author(a):
        if not a:
            return ""
        a = a.replace("(tổng hợp)", "")
        a = re.sub(r"[^a-zA-ZÀ-ỹ\s]", "", a)
        return " ".join(w.capitalize() for w in a.split())

    @staticmethod
    def parse_published_date(raw):
        """Parse published_at từ staging (tiếng Việt + offset)"""
        if not raw:
            return None
        s = raw.strip()
        
        # Bỏ tên ngày trong tuần (Thứ 2, Thứ 3, Thứ bảy, etc.)
        s = re.sub(r"^[^\d]*,\s*", "", s)
        
        # Bỏ (GMT+7)
        s = re.sub(r"\(GMT[+-]\d+\)", "", s).strip()
        
        # Thử các định dạng
        fmts = [
            "%d/%m/%Y, %H:%M",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y"
        ]
        for f in fmts:
            try:
                return datetime.strptime(s, f)
            except:
                continue
        
        # Nếu không parse được, thử tách ngày/giờ thủ công
        match = re.search(r"(\d{1,2}/\d{1,2}/\d{4})[, ]+(\d{1,2}:\d{2})?", s)
        if match:
            date_part = match.group(1)
            time_part = match.group(2) or "00:00"
            try:
                return datetime.strptime(f"{date_part} {time_part}", "%d/%m/%Y %H:%M")
            except:
                return None
        return None


    @staticmethod
    def parse_scraped_date(raw):
        """Parse scraped_at từ staging (TEXT, có thể có microseconds)"""
        if not raw:
            return None
        s = raw.strip().split(".")[0]  # bỏ microseconds
        fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"]
        for f in fmts:
            try:
                return datetime.strptime(s, f)
            except:
                continue
        return None

    @staticmethod
    def format_tags(raw):
        """Trả về chuỗi các tag, ví dụ: 'tag1, tag2, tag3'"""
        if not raw:
            return ""
        arr = [x.strip() for x in raw.split(",") if x.strip()]
        arr = list(dict.fromkeys(arr))  # unique
        return ", ".join(arr)

    # ===== Transform row =====
    def transform_row(self, r):
        if not r.get("article_url"):
            return None

        empty_fields = [
            "source_name", "category", "author",
            "title", "summary", "content",
            "tags", "published_at"
        ]

        # bỏ row có quá 2 trường rỗng
        if sum(self.is_empty(r.get(c)) for c in empty_fields) > 2:
            return None

        # normalize
        source = SOURCE_MAP.get((r.get("source_name") or "").lower(), r.get("source_name") or "")
        category = CATEGORY_MAP.get((r.get("category") or "").lower(), r.get("category") or "")
        author = self.normalize_author(r.get("author"))

        published_at_dt = self.parse_published_date(r.get("published_at"))
        scraped_at_dt = self.parse_scraped_date(r.get("scraped_at"))

        title = self.clean_html(r.get("title"))
        description = self.clean_html(r.get("summary"))
        content = r.get("content") or ""
        tags = self.format_tags(r.get("tags"))

        # ===== TRẢ VỀ ĐÚNG 11 TRƯỜNG CỦA transformed_temp_table =====
        return (
            r.get("article_url"),
            source,
            category,
            author,
            published_at_dt,
            title,
            description,
            content,
            scraped_at_dt,
            r.get("run_id"),
            tags
        )

    # ===== DB operations =====
    def truncate_table(self):
        self.cursor.execute("TRUNCATE TABLE transformed_temp_table")
        self.conn.commit()

    def batch_insert(self, rows):
        payload = []

        for r in rows:
            published_str = r[4].strftime("%Y-%m-%d %H:%M:%S") if r[4] else None
            scraped_str = r[8].strftime("%Y-%m-%d %H:%M:%S") if r[8] else None

            payload.append({
                "article_url": r[0],
                "source_name": r[1],
                "category_name": r[2],
                "author_name": r[3],
                "published_at": published_str,
                "title": r[5],
                "description": r[6],
                "content": r[7],
                "scraped_at": scraped_str,
                "run_id": r[9],
                "tags": r[10],
            })

        json_payload = json.dumps(payload, ensure_ascii=False)
        sql = "CALL sp_insert_transformed_batch(%s)"
        self.cursor.execute(sql, (json_payload,))
        self.conn.commit()

        print(f"Inserted {len(rows)} rows (batch JSON → stored procedure)")

    # ===== ETL =====
    def run_etl(self):
        data = self.fetch_staging_data()
        print(f"Staging rows: {len(data)}")

        rows = []
        for r in data:
            t = self.transform_row(r)
            if t:
                rows.append(t)

        print(f"Valid rows: {len(rows)}")

        # sort theo published_at, None ở cuối
        rows.sort(key=lambda x: (x[4] is None, x[4]))

        if rows:
            self.truncate_table()
            self.batch_insert(rows)
        else:
            print("No valid data")

    def close(self):
        self.cursor.close()
        self.conn.close()


# ===== Run script =====
if __name__ == "__main__":
    loader = TransformLoader()
    try:
        loader.run_etl()
    finally:
        loader.close()
