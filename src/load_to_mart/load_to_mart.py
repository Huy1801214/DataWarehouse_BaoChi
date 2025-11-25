# load_to_datamart.py (Bước Load Data Mart)

import os
import uuid
import pandas as pd
from datetime import date
from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end

# --- Cấu hình File & Constants ---
TODAY_STR = date.today().strftime("%Y%m%d")
BASE_DIR = os.path.dirname(os.path.abspath(__file__)) 
SOURCE_DIR = os.path.join(BASE_DIR, "..", "source")

# Định nghĩa thứ tự Load (Dims trước, Aggregate sau)
LOAD_ORDER_INFO = [
    ("Agg_Mart_Top_Trends", f"agg_mart_data_{TODAY_STR}.csv", "Agg_Mart_Top_Trends"),
    ("DimCategory", f"dim_category_{TODAY_STR}.csv", "DimCategory"),
    ("DimTag", f"dim_tag_{TODAY_STR}.csv", "DimTag"),
    ("DimSource", f"dim_source_{TODAY_STR}.csv", "DimSource") 
]

def get_create_table_query(prod_table_name, temp_table_name):
    """Tạo lệnh CREATE TABLE LIKE để tạo bảng tạm dựa trên cấu trúc bảng Production."""
    return f"CREATE TABLE {temp_table_name} LIKE {prod_table_name}"

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
    temp_table_id = f"{uuid.uuid4().hex[:8]}" # ID duy nhất cho phiên chạy
    rename_commands = []
    current_table_name = "" # Dùng để ghi log lỗi chính xác

    try:
        conn_mart = connect_to_db("news_mart_db")
        if not conn_mart: raise Exception("Không thể kết nối Data Mart.")
        cursor_mart = conn_mart.cursor()

        # 1. Load lần lượt các bảng vào các bảng tạm mới
        for table_name_short, file_name, prod_name in LOAD_ORDER_INFO:
            current_table_name = table_name_short
            file_path = os.path.join(SOURCE_DIR, file_name)
            temp_table_name = f"{prod_name}_temp_{temp_table_id}"
            
            if not os.path.exists(file_path):
                raise FileNotFoundError(f"Thiếu file: {file_name}")

            df = pd.read_csv(file_path, encoding='utf8', keep_default_na=False)
            extracted = len(df)

            # XỬ LÝ DỮ LIỆU & TÊN CỘT (Fix NaN/Missing Headers)
            # Chuyển đổi tất cả các giá trị NaN (tức là ô trống) thành None 
            # để MySQL chấp nhận NULL
            df = df.where(pd.notna(df), None)
            df.columns = [str(col).strip() for col in df.columns]
            valid_cols = [col for col in df.columns if col.lower() not in ('nan', 'unnamed: 0') and col != '']

            # 2. CREATE TABLE LIKE (Tạo bảng Tạm)
            create_query = get_create_table_query(prod_name, temp_table_name)
            print(f"-> 1. Tạo bảng tạm {temp_table_name}...")
            cursor_mart.execute(create_query)

            # 3. INSERT DỮ LIỆU VÀO BẢNG TẠM
            print(f"-> Nạp {extracted} dòng vào {temp_table_name}...")
            insert_query = get_insert_query(temp_table_name, valid_cols)
            data_to_insert = [tuple(row) for row in df[valid_cols].values]
            
            cursor_mart.executemany(insert_query, data_to_insert)
            total_records_loaded += extracted

            # 4. CHUẨN BỊ LỆNH RENAME
            rename_commands.append(f"RENAME TABLE {temp_table_name} TO {prod_name}")

        print("\n=== BẮT ĐẦU HOÁN ĐỔI SCHEMA (Zero Downtime) ===")
        # 5. DROP bảng Production cũ và RENAME bảng tạm mới   
        for cmd in rename_commands:
            prod_name = cmd.split(' TO ')[1]
            cursor_mart.execute(f"DROP TABLE IF EXISTS {prod_name}")
            cursor_mart.execute(cmd) 

        conn_mart.commit()
        
        # 6. Ghi log END SUCCESS
        log_end(run_id_load, "SUCCESS", records_extracted=total_records_loaded, records_loaded=total_records_loaded)
        print(f"Load Mart SUCCESS. Tổng cộng {total_records_loaded} bản ghi đã nạp.")

    except Exception as e:
        # Nếu lỗi xảy ra, Rollback toàn bộ giao dịch INSERT/CREATE TABLE
        error_msg = f"Lỗi Load Data Mart (Table {current_table_name}): {str(e)}"

        # !!! BƯỚC SỬA LỖI THỨ 2: DỌN DẸP BẢNG TẠM CÒN SÓT !!!
        print(">>> BẮT ĐẦU DỌN DẸP CÁC BẢNG TẠM CÒN SÓT <<<")
        for cmd in rename_commands:
            temp_name = cmd.split(' RENAME TABLE ')[1].split(' TO ')[0]
            try:
                cursor_mart.execute(f"DROP TABLE IF EXISTS {temp_name}")
                print(f"-> Đã xóa bảng tạm: {temp_name}")
            except Exception as cleanup_e:
                print(f"Lỗi dọn dẹp: {cleanup_e}")
                
        if conn_mart: conn_mart.rollback()
        log_end(run_id_load, "FAIL", records_extracted=extracted, records_loaded=total_records_loaded, error_message=error_msg)
        print(f"FAIL: {error_msg}.")

    finally:
        if conn_mart: conn_mart.close()

if __name__ == "__main__":
    run_load_mart_job()