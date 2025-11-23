import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore")

from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end

def export():
    JOB_NAME = 'export_staging_file'
    CONFIG_ID = None
    
    # 1. GHI LOG START
    start_result = log_start(JOB_NAME, CONFIG_ID)
    
    # Xử lý lấy run_id 
    if start_result and isinstance(start_result, tuple):
        run_id = start_result[0]
    else:
        run_id = start_result

    if not run_id:
        print("Không khởi tạo được Run ID.")
        return

    conn = connect_to_db("news_staging_db")
    if not conn: 
        log_end(run_id, "FAILED", 0, 0, "Connection Failed")
        return
    
    try:
        print(f"[RunID: {run_id}] Đang xuất dữ liệu ra file CSV...")
        
        # 2. Lấy dữ liệu
        df = pd.read_sql("SELECT * FROM staging_delta", conn)
        row_count = len(df)
        
        # 3. Kiểm tra và tạo thư mục (Tránh lỗi OSError)
        output_dir = "src/source"
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        output_file = f"{output_dir}/delta_data.csv"
        
        # 4. Xuất file
        df.to_csv(output_file, index=False, header=False)
        print(f"Đã xuất {row_count} dòng ra file: {output_file}")
        
        # 5. GHI LOG THÀNH CÔNG
        # records_extracted = số dòng đọc từ DB
        # records_loaded = số dòng ghi ra file (bằng nhau)
        log_end(run_id, "SUCCESS", records_extracted=row_count, records_loaded=row_count)
        
    except Exception as e:
        err_msg = str(e)
        print(f"Lỗi Export: {err_msg}")
        log_end(run_id, "FAILED", 0, 0, err_msg)
        
    finally:
        if conn: conn.close()

if __name__ == "__main__":
    export()