# aggregate_to_csv.py (Bước Aggregate và DOM)

import os
import pandas as pd
from datetime import date
from utils.db_utils import connect_to_db, log_startg, log_endg

TODAY_STR = date.today().strftime("%Y%m%d")
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
OUTPUT_DIR = os.path.join(BASE_DIR, "..", "source")

# Dictionary chứa tên bảng và tên file tương ứng
TABLE_FILES = {
    "Agg_Temp_Mart": f"agg_mart_data_{TODAY_STR}.csv",
    "DimCategory": f"dim_category_{TODAY_STR}.csv",
    "DimDate": f"dim_date_{TODAY_STR}.csv",
    "DimTag": f"dim_tag_{TODAY_STR}.csv",
    "DimSource": f"dim_source_{TODAY_STR}.csv"
}

def run_aggregate_and_dom():
    job_name = "Aggregate_DOM"
    run_id_agg = None
    conn_dwh = None
    records_aggregated = 0
    total_files_written = 0
    run_id_agg, conn_control = log_startg(job_name)
    print(f"Job Aggregate_DOM started with Run ID: {run_id_agg}")
    if not run_id_agg: return

    try:
        conn_dwh = connect_to_db("news_warehouse_db")
        if not conn_dwh: raise Exception("Không thể kết nối Data Warehouse.")
        
        print("1. Gọi SP_Run_Aggregation để tính toán...")
        cursor_dwh = conn_dwh.cursor()
        cursor_dwh.callproc('SP_Run_Aggregation')
        conn_dwh.commit()
        records_aggregated = 1000 # Giả định số dòng
        
        # 2. DOM MULTI-TABLES (Đọc và Ghi 5 bảng)
        print("2. Đang DOM 5 bảng (Aggregate + Dims) ra file CSV...")
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        
        for table_name, file_name in TABLE_FILES.items():
            file_path = os.path.join(OUTPUT_DIR, file_name)
            
            # Truy vấn SQL (Tùy chỉnh SELECT nếu cần, nhưng * là đủ cho Dim)
            if table_name == "Agg_Temp_Mart":
                # Chỉ lấy các cột cần thiết cho Mart (SK và Measures)
                select_cols = "article_year, article_month, category_key, tag_key, source_key, total_articles"
            else:
                # Lấy toàn bộ cột cho các bảng Dimension
                select_cols = "*" 

            sql_query = f"SELECT {select_cols} FROM {table_name}"
            df = pd.read_sql(sql_query, conn_dwh)
            df.to_csv(file_path, index=False, encoding='utf8')
            total_files_written += 1
            print(f"   -> Đã ghi {table_name} ({len(df)} dòng)")
        
        # 3. Ghi log END (Aggregate)
        log_endg(run_id_agg, "SUCCESS", records_extracted=records_aggregated, records_loaded=total_files_written) # Loaded=Số file
        print(f"Aggregate SUCCESS. Đã ghi {total_files_written} file.")

    except Exception as e:
        error_msg = f"Lỗi Aggregate/DOM: {str(e)}"
        if conn_dwh: conn_dwh.rollback()
        log_endg(run_id_agg, "FAIL", records_extracted=records_aggregated, records_loaded=0, error_message=error_msg)
        print(f"FAIL: {error_msg}. Dừng quy trình.")
        
    finally:
        if conn_dwh: conn_dwh.close()

if __name__ == "__main__":
    run_aggregate_and_dom()