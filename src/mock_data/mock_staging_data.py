from utils.db_utils import connect_to_db
import uuid
from datetime import datetime

conn = connect_to_db("news_staging_db")
cur = conn.cursor()

sample_data = [
    ("https://vnexpress.net/test-1", "VnExpress", "Thời sự", "Nguyễn Văn A", 
     "Bài viết test 1", "Tóm tắt 1", "Nội dung bài viết test 1", datetime.now(), str(uuid.uuid4())),
    ("https://thanhnien.vn/test-2", "ThanhNien", "Giáo dục", "Trần Thị B", 
     "Bài viết test 2", "Tóm tắt 2", "Nội dung bài viết test 2", datetime.now(), str(uuid.uuid4())),
]

cur.executemany("""
    INSERT INTO staging_temp (
        article_url, source_name, category_raw, author_raw,
        title_raw, summary_raw, content_raw, scraped_at, run_id
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
""", sample_data)

conn.commit()
cur.close()
conn.close()
print("Đã thêm dữ liệu mẫu vào staging_temp")
