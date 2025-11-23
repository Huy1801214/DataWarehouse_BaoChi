# load_to_datamart.py (Bước Load Data Mart)

import os
import pandas as pd
from datetime import date
from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end

# --- Cấu hình File & Constants ---
TODAY_STR = date.today().strftime("%Y%m%d")
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
SOURCE_DIR = os.path.join(BASE_DIR, "..", "source")

# Định nghĩa thứ tự Load (Dims trước, Fact sau)
LOAD_ORDER = [
    ("DimDate", f"dim_date_{TODAY_STR}.csv"),
    ("DimCategory", f"dim_category_{TODAY_STR}.csv"),
    ("DimTag", f"dim_tag_{TODAY_STR}.csv"),
    ("DimSource", f"dim_source_{TODAY_STR}.csv"),
    ("Agg_Mart_Top_Trends", f"agg_mart_data_{TODAY_STR}.csv") 
]

def get_insert_query(table_name, df_cols):
    """Tạo INSERT query động dựa trên tên bảng và cột."""
    cols = ", ".join(df_cols)
    placeholders = ", ".join(["%s"] * len(df_cols))
    return f"INSERT INTO {table_name} ({cols}) VALUES ({placeholders})"

def run_load_mart_job():
    job_name = "Load_DataMart"
    run_id_load, conn_control = log_start(job_name)
    print(f"Run ID Load Data Mart: {run_id_load}")
    if not run_id_load: return
        
    conn_mart = None
    total_records_loaded = 0
    extracted = 0
    
    try:
        conn_mart = connect_to_db("news_mart_db")
        if not conn_mart: raise Exception("Không thể kết nối Data Mart.")
        cursor_mart = conn_mart.cursor()

         # 1.CALL SP_Clear_Mart (Ghi đè tất cả 5 bảng)
        print("Đang gọi SP để dọn dẹp toàn bộ Data Mart...")
        cursor_mart.callproc('SP_Clear_Mart')
        
        # 2. Load lần lượt các bảng theo thứ tự
        for table_name, file_name in LOAD_ORDER:
            file_path = os.path.join(SOURCE_DIR, file_name)
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Thiếu file: {file_name}")

            df = pd.read_csv(file_path, encoding='utf8', keep_default_na=False)
            extracted = len(df)

            # Chuyển đổi tất cả các giá trị NaN (tức là ô trống) thành None 
            # để MySQL chấp nhận NULL
            df = df.where(pd.notna(df), None)
            df.columns = [str(col).strip() for col in df.columns]
            valid_cols = [col for col in df.columns if col.lower() not in ('nan', 'unnamed: 0') and col != '']

            # --- INSERT DỮ LIỆU ---
            print(f"-> Nạp {extracted} dòng vào {table_name}...")
            
            insert_query = get_insert_query(table_name, valid_cols)
            data_to_insert = [tuple(row) for row in df[valid_cols].values]
            
            cursor_mart.executemany(insert_query, data_to_insert)
            total_records_loaded += cursor_mart.rowcount
            
        conn_mart.commit()
        
        # 2. Ghi log END (Load Mart)
        log_end(run_id_load, "SUCCESS", records_extracted=total_records_loaded, records_loaded=total_records_loaded)
        print(f"Load Mart SUCCESS. Tổng cộng {total_records_loaded} bản ghi đã nạp.")

    except Exception as e:
        error_msg = f"Lỗi Load Data Mart (Table {table_name}): {str(e)}"
        if conn_mart: conn_mart.rollback()
        log_end(run_id_load, "FAIL", records_extracted=extracted, records_loaded=total_records_loaded, error_message=error_msg)
        print(f"FAIL: {error_msg}.")

    finally:
        if conn_mart: conn_mart.close()

if __name__ == "__main__":
    run_load_mart_job()