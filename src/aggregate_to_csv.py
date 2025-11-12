# aggregate_to_csv.py (Bước Aggregate và DOM)

import os
import mysql.connector
import pandas as pd
from datetime import date
# --- Tải các hàm quản lý từ db_connect.py ---
from utils.db_utils import start_job_log, end_job_log, connect_to_db, log_error, load_config

# --- Cấu hình File ---
TODAY_STR = date.today().strftime("%Y%m%d")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
TEMP_CSV_FILE = os.path.join(BASE_DIR, "..", "temp", f"agg_mart_data_{TODAY_STR}.csv")

# --- Truy vấn Tổng hợp Phức tạp ---
AGGREGATION_QUERY = """
INSERT INTO news_warehouse_db.Agg_Temp_Mart (
    date_key, category_key, tag_key, article_year, article_month, 
    category_name, tag_name, total_articles, avg_sentiment
)
SELECT
    fam.datekey, fam.categorykey, fat.tag_key, dd.year, dd.month,
    dc.categoryname, dt.tag_name,
    COUNT(fam.articlekey) AS total_articles,
    AVG(fam.sentimentscore) AS avg_sentiment
FROM
    news_warehouse_db.FactArticleMetrics fam 
JOIN news_warehouse_db.FactArticleTags fat ON fam.articlekey = fat.article_key
JOIN news_warehouse_db.DimDate dd ON fam.datekey = dd.datekey
JOIN news_warehouse_db.DimCategory dc ON fam.categorykey = dc.categorykey
JOIN news_warehouse_db.DimTag dt ON fat.tag_key = dt.tag_key
GROUP BY
    fam.datekey, fam.categorykey, fat.tag_key, dd.year, dd.month, 
    dc.categoryname, dt.tag_name;
"""

def run_aggregate_and_dom():
    job_name = "Aggregate_DOM"
    # Ghi log START
    run_id, conn_control = start_job_log(job_name)
    if not run_id: return

    conn_dwh = None
    records_processed = 0

    try:
        # Load config (Nếu cần tham số đặc biệt cho Aggregate)
        # Ví dụ: configs = load_config("AGGREGATE_JOB") 
        
        conn_dwh = connect_to_db("news_warehouse_db")
        if not conn_dwh: raise Exception("Không thể kết nối Data Warehouse.")

        # 1. TRUNCATE và GHI vào Bảng Tạm Aggregate trong DW
        cursor_dwh = conn_dwh.cursor()
        print("1. Đang TRUNCATE bảng Agg_Temp_Mart và chạy truy vấn tổng hợp...")
        cursor_dwh.execute("TRUNCATE TABLE news_warehouse_db.Agg_Temp_Mart")
        cursor_dwh.execute(AGGREGATION_QUERY)
        conn_dwh.commit()
        records_processed = cursor_dwh.rowcount

        # 2. DOM dữ liệu ra File CSV
        print("2. Đang DOM dữ liệu từ bảng tạm DW ra file CSV...")
        df = pd.read_sql("SELECT * FROM news_warehouse_db.Agg_Temp_Mart", conn_dwh)
        
        os.makedirs(os.path.dirname(TEMP_CSV_FILE), exist_ok=True)
        df.to_csv(TEMP_CSV_FILE, index=False, encoding='utf8')
        
        # Ghi log SUCCESS
        end_job_log(run_id, "SUCCESS", records_extracted=records_processed) 
        print(f"Hoàn thành Aggregate và DOM {records_processed} dòng vào CSV.")

    except Exception as e:
        error_msg = f"Lỗi Aggregate/DOM: {str(e)}"
        print(f"{error_msg}")
        if conn_dwh: conn_dwh.rollback()
        log_error(run_id, error_msg)
    finally:
        if conn_dwh: conn_dwh.close()

if __name__ == "__main__":
    run_aggregate_and_dom()