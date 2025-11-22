# load_to_datamart.py (Bước Load Data Mart)

import os
import mysql.connector
import pandas as pd
from datetime import date
from utils.db_utils import start_job_log, end_job_log, connect_to_db, log_error

# --- Cấu hình File ---
TODAY_STR = date.today().strftime("%Y%m%d")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CSV_SOURCE = os.path.join(BASE_DIR, "..", "temp", f"agg_mart_data_{TODAY_STR}.csv")

def run_load_mart_job():
    job_name = "Load_To_DataMart"
    # Ghi log START
    run_id, conn_control = start_job_log(job_name)
    if not run_id: return

    conn_mart = None
    records_loaded = 0
    
    try:
        if not os.path.exists(CSV_SOURCE):
            raise FileNotFoundError(f"Không tìm thấy file Aggregate CSV: {CSV_SOURCE}")
            
        # 1. Đọc dữ liệu từ File CSV
        df = pd.read_csv(CSV_SOURCE, encoding='utf8')
        
        # 2. Kết nối và Ghi vào Data Mart
        conn_mart = connect_to_db("news_mart_db")
        if not conn_mart: return

        cursor_mart = conn_mart.cursor()
        
        # Chiến lược TRUNCATE và LOAD
        print("Đang TRUNCATE bảng Agg_Mart_Top_Trends...")
        cursor_mart.execute("TRUNCATE TABLE news_mart_db.Agg_Mart_Top_Trends")
        
        # Chuẩn bị dữ liệu để nạp
        data_to_insert = [tuple(row) for row in df.values]
        
        INSERT_MART_QUERY = """
        INSERT INTO Agg_Mart_Top_Trends 
        (date_key, category_key, tag_key, article_year, article_month, category_name, tag_name, total_articles, avg_sentiment) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        cursor_mart.executemany(INSERT_MART_QUERY, data_to_insert)
        conn_mart.commit()
        records_loaded = cursor_mart.rowcount
        
        # Ghi log SUCCESS
        end_job_log(run_id, "SUCCESS", records_loaded=records_loaded, records_extracted=len(df))
        print(f"✅ Hoàn thành Load Mart: {records_loaded} dòng đã nạp.")

    except Exception as e:
        error_msg = f"Lỗi Load Data Mart: {str(e)}"
        print(f"❌ {error_msg}")
        if conn_mart: conn_mart.rollback()
        log_error(run_id, error_msg)
    finally:
        if conn_mart: conn_mart.close()

if __name__ == "__main__":
    run_load_mart_job()