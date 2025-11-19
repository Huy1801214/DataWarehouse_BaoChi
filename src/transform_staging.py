import re
import json
from datetime import datetime, date
from db_connect import connect_to_db

# ============================
# Chuẩn hóa Source & Category
# ============================

SOURCE_MAP = {
    "vnexpress": "VnExpress",
    "dantri": "Dân Trí",
    "tuoitre": "Tuổi Trẻ",
    "thanhnien": "Thanh Niên",
    "laodong": "Lao Động"
}

CATEGORY_MAP = {
    "xa hoi": "Xã hội",
    "thoi su": "Thời sự",
    "the gioi": "Thế giới",
    "kinh doanh": "Kinh doanh",
    "giao duc": "Giáo dục",
    "phap luat": "Pháp luật",
}

# ============================
# Chuẩn hóa tác giả
# ============================

def normalize_author(name):
    if not name:
        return ""
    name = name.replace("(tổng hợp)", "").strip().lower()
    name = re.sub(r"[^a-zA-ZÀ-ỹ\s]", "", name)
    return " ".join(w.capitalize() for w in name.split())

# ============================
# Chuẩn hóa tags
# ============================

def normalize_tags(raw_tags):
    if not raw_tags:
        return json.dumps([], ensure_ascii=False)
    tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    tags = list(dict.fromkeys(tags))
    return json.dumps(tags, ensure_ascii=False)

# ============================
# Sentiment Analysis đơn giản
# ============================

POS_WORDS = ["tốt", "vui", "tăng", "thuận lợi", "phát triển", "lợi nhuận"]
NEG_WORDS = ["xấu", "giảm", "thiệt hại", "bất lợi", "khó khăn", "sập"]

def calc_sentiment(text):
    if not text:
        return 0.0
    score = 0
    text_lower = text.lower()
    for w in POS_WORDS:
        if w in text_lower:
            score += 1
    for w in NEG_WORDS:
        if w in text_lower:
            score -= 1
    return float(score)

# ============================
# Parse published_at chuẩn VnExpress
# ============================

def parse_published_at(raw):
    if not raw:
        return None
    s = raw.strip()
    # Bỏ thứ trong tuần, ví dụ "Thứ bảy,"
    s = re.sub(r"^[^\d]+,\s*", "", s)
    # Chuyển (GMT+7) -> +0700
    s = s.replace("GMT+7", "+0700").replace("(", "").replace(")", "")
    formats = [
        "%d/%m/%Y, %H:%M %z",
        "%d/%m/%Y, %H:%M",
        "%d/%m/%Y %H:%M",
        "%d/%m/%Y"
    ]
    for fmt in formats:
        try:
            return datetime.strptime(s, fmt)
        except:
            pass
    return None

# ============================
# CLASS chính
# ============================

class TransformLoader:
    def __init__(self, db_name="news_staging_db"):
        self.conn = connect_to_db(db_name)
        if not self.conn:
            raise ConnectionError("Không thể kết nối database.")
        self.cursor = self.conn.cursor(dictionary=True)

    def fetch_staging_data(self):
        self.cursor.execute("SELECT * FROM staging_temp_table")
        return self.cursor.fetchall()

    def clean_text(self, text):
        if not text:
            return ""
        return re.sub(r"<[^>]*>", "", text).strip()

    def transform_articles(self, data):
        transformed = []
        for item in data:

            # Chuẩn hóa source
            raw_source = (item.get("source_name_raw") or "").strip().lower()
            source_name = SOURCE_MAP.get(raw_source, raw_source.title())

            # Chuẩn hóa category
            raw_cat = (item.get("category_raw") or "").strip().lower()
            category_name = CATEGORY_MAP.get(raw_cat, raw_cat.title())

            # Chuẩn hóa tác giả
            author_name = normalize_author(item.get("author_raw") or "")

            # Văn bản
            title = self.clean_text(item.get("title_raw"))
            description = self.clean_text(item.get("summary_raw"))
            content = item.get("content_raw") or ""

            # Word count
            word_count = len(content.split())

            # Tags
            tags = normalize_tags(item.get("tags_raw"))

            # Ngày đăng
            published_at = parse_published_at(item.get("published_at_raw"))
            if not published_at:
                # fallback: dùng date_dim nếu parse fail
                published_at = item.get("date_dim")

            # Sentiment
            sentiment_score = calc_sentiment(content)

            transformed.append({
                "article_url": item.get("article_url"),
                "source_name": source_name,
                "category_name": category_name,
                "author_name": author_name,
                "published_at": published_at,
                "title": title,
                "description": description,
                "word_count": word_count,
                "tags": tags,
                "sentiment_score": sentiment_score,
                "run_id": item.get("run_id"),
                "date_dim": item.get("date_dim") or date.today()
            })
        return transformed

    def clear_today_transformed(self):
        today = date.today()
        delete_query = "DELETE FROM transformed_temp_table WHERE date_dim = %s"
        self.cursor.execute(delete_query, (today,))
        self.conn.commit()
        print(f"Đã xóa dữ liệu cũ của ngày {today} trong transformed_temp_table")

    def load_transformed_data(self, transformed_data):
        insert_query = """
            INSERT INTO transformed_temp_table (
                article_url, source_name, category_name, author_name,
                published_at, title, description, word_count, tags,
                sentiment_score, run_id, date_dim
            ) VALUES (
                %(article_url)s, %(source_name)s, %(category_name)s, %(author_name)s,
                %(published_at)s, %(title)s, %(description)s, %(word_count)s,
                %(tags)s, %(sentiment_score)s, %(run_id)s, %(date_dim)s
            )
        """
        try:
            self.cursor.executemany(insert_query, transformed_data)
            self.conn.commit()
            print(f"Đã insert {self.cursor.rowcount} bản ghi vào transformed_temp_table")
            return True
        except Exception as e:
            print("Lỗi khi insert dữ liệu vào transformed_temp_table:")
            print(str(e))
            self.conn.rollback()
            return False

    def close(self):
        self.cursor.close()
        self.conn.close()


if __name__ == "__main__":
    loader = TransformLoader()
    try:
        loader.clear_today_transformed()
        staging_data = loader.fetch_staging_data()
        if not staging_data:
            print("Không có dữ liệu trong staging_temp_table.")
        else:
            transformed = loader.transform_articles(staging_data)
            success = loader.load_transformed_data(transformed)
            if not success:
                print("Không lưu được dữ liệu transform vào transformed_temp_table.")
    finally:
        loader.close()
