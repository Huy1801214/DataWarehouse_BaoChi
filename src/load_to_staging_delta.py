import os
import mysql.connector
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore") 
from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end

def run_incremental_etl():
    JOB_NAME = 'load_delta'
    CONFIG_ID = None 
    
    # 1. GHI LOG START 
    start_result = log_start(JOB_NAME, CONFIG_ID)
    
    if start_result and isinstance(start_result, tuple):
        run_id = start_result[0] # Lấy ID 
    else:
        run_id = start_result
    if not run_id:
        print("Không khởi tạo được Run ID. Dừng chương trình.")
        return

    conn = connect_to_db("news_staging_db")
    if conn is None:
        log_end(run_id, "FAILED", 0, 0, "Connection Failed")
        return 

    try:
        cursor = conn.cursor()
        print(f"[RunID: {run_id}] Đang gọi Procedure...")
        
        cursor.callproc('load_to_staging_delta')
        
        result_value = None
        for result in cursor.stored_results():
            row = result.fetchone()
            if row: result_value = row[0]

        conn.commit()

        # --- XỬ LÝ KẾT QUẢ ---
        if isinstance(result_value, str):
            err_msg = f"SQL Error: {result_value}"
            print(f"{err_msg}")
            log_end(run_id, "FAILED", 0, 0, err_msg)
        
        elif isinstance(result_value, int):
            print(f"Thành công! Load {result_value} dòng.")
            # Với bước này: extracted = loaded (lấy bao nhiêu nạp bấy nhiêu)
            log_end(run_id, "SUCCESS", records_extracted=result_value, records_loaded=result_value)
        
        else:
            msg = "Procedure returned None"
            print(f"{msg}")
            log_end(run_id, "FAILED", 0, 0, msg)

    except mysql.connector.Error as err:
        err_msg = f"Python Error: {err}"
        print(f"{err_msg}")
        log_end(run_id, "FAILED", 0, 0, err_msg)
        
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()
            print("Đã đóng kết nối.")

if __name__ == "__main__":
    run_incremental_etl()