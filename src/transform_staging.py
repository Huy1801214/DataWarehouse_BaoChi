import re
from datetime import datetime
from utils.db_utils import connect_to_db
import json
import uuid

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

class TransformLoader:
    """Transform data từ staging → transformed_temp_table + logging + config_id tự động"""

    def __init__(self, staging_db="news_staging_db", control_db="news_control_db"):
        self.staging_conn = connect_to_db(staging_db)
        self.staging_cursor = self.staging_conn.cursor(dictionary=True)

        self.control_conn = connect_to_db(control_db)
        self.control_cursor = self.control_conn.cursor()

        self.run_id = str(uuid.uuid4())
        self.config_id = None

    # ===========================
    # Config helper
    # ===========================
    def get_or_create_config(self, source_name):
        select_query = "SELECT config_id FROM config_table WHERE source_name=%s"
        self.control_cursor.execute(select_query, (source_name,))
        row = self.control_cursor.fetchone()
        if row:
            self.config_id = row[0]
        else:
            insert_query = """
                INSERT INTO config_table (source_name, active, updated_at)
                VALUES (%s, %s, %s)
            """
            self.control_cursor.execute(insert_query, (source_name, True, datetime.now()))
            self.control_conn.commit()
            self.config_id = self.control_cursor.lastrowid
        return self.config_id

    # ===========================
    # Logging helper
    # ===========================
    def create_log(self, job_name="transform_loader"):
        insert_log = """
            INSERT INTO logging_table (run_id, config_id, job_name, status, start_time)
            VALUES (%s, %s, %s, %s, %s)
        """
        self.control_cursor.execute(insert_log, (self.run_id, self.config_id, job_name, "running", datetime.now()))
        self.control_conn.commit()

    def update_log(self, status="SUCCESS", records_extracted=0, records_loaded=0, error_message=None):
        update_log = """
            UPDATE logging_table
            SET status=%s,
                end_time=%s,
                records_extracted=%s,
                records_loaded=%s,
                error_message=%s
            WHERE run_id=%s
        """
        self.control_cursor.execute(update_log, (status, datetime.now(), records_extracted, records_loaded, error_message, self.run_id))
        self.control_conn.commit()

    # ===========================
    # Transform helpers
    # ===========================
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
        if not raw:
            return None
        s = raw.strip()
        s = re.sub(r"^[^\d]*,\s*", "", s)
        s = re.sub(r"\(GMT[+-]\d+\)", "", s).strip()
        fmts = ["%d/%m/%Y, %H:%M", "%d/%m/%Y %H:%M", "%d/%m/%Y"]
        for f in fmts:
            try:
                return datetime.strptime(s, f)
            except:
                continue
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
        if not raw:
            return None
        s = raw.strip().split(".")[0]
        fmts = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%d/%m/%Y %H:%M:%S", "%d/%m/%Y"]
        for f in fmts:
            try:
                return datetime.strptime(s, f)
            except:
                continue
        return None

    @staticmethod
    def format_tags(raw):
        if not raw:
            return ""
        arr = [x.strip() for x in raw.split(",") if x.strip()]
        arr = list(dict.fromkeys(arr))
        return ", ".join(arr)

    def transform_row(self, r):
        if not r.get("article_url"):
            return None
        empty_fields = ["source_name", "category", "author", "title", "summary", "content", "tags", "published_at"]
        if sum(self.is_empty(r.get(c)) for c in empty_fields) > 2:
            return None
        source = SOURCE_MAP.get((r.get("source_name") or "").lower(), r.get("source_name") or "")
        category = CATEGORY_MAP.get((r.get("category") or "").lower(), r.get("category") or "")
        author = self.normalize_author(r.get("author"))
        published_at_dt = self.parse_published_date(r.get("published_at"))
        scraped_at_dt = self.parse_scraped_date(r.get("scraped_at"))
        title = self.clean_html(r.get("title"))
        description = self.clean_html(r.get("summary"))
        content = r.get("content") or ""
        tags = self.format_tags(r.get("tags"))
        return (r.get("article_url"), source, category, author, published_at_dt, title, description, content, scraped_at_dt, r.get("run_id"), tags)

    # ===========================
    # DB operations
    # ===========================
    def fetch_staging_data(self):
        self.staging_cursor.execute("SELECT * FROM staging_temp_table")
        return self.staging_cursor.fetchall()

    def truncate_table(self):
        self.staging_cursor.execute("TRUNCATE TABLE transformed_temp_table")
        self.staging_conn.commit()

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
        self.staging_cursor.execute(sql, (json_payload,))
        self.staging_conn.commit()
        return len(rows)

    # ===========================
    # ETL
    # ===========================
    def run_etl(self):
        try:
            # Lấy source_name đầu tiên để get/create config
            staging_data = self.fetch_staging_data()
            if not staging_data:
                print("No staging data found")
                return

            first_source = staging_data[0].get("source_name") or "unknown_source"
            self.get_or_create_config(first_source)

            # Tạo log
            self.create_log(job_name="transform_loader")

            records_extracted = len(staging_data)
            rows = [r for r in (self.transform_row(r) for r in staging_data) if r]
            records_loaded = len(rows)

            print(f"Staging rows: {records_extracted}, Valid rows: {records_loaded}")

            if rows:
                self.truncate_table()
                self.batch_insert(rows)

            self.update_log(status="SUCCESS", records_extracted=records_extracted, records_loaded=records_loaded)
        except Exception as e:
            self.update_log(status="failed", error_message=str(e))
            print("ETL failed:", str(e))

    def close(self):
        self.staging_cursor.close()
        self.staging_conn.close()
        self.control_cursor.close()
        self.control_conn.close()


# ===== Run script =====
if __name__ == "__main__":
    loader = TransformLoader()
    try:
        loader.run_etl()
    finally:
        loader.close()
