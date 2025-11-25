import os
import mysql.connector
from dotenv import load_dotenv
import warnings
import sys
warnings.filterwarnings("ignore") 
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end

def run_incremental_etl():
    # 1. KHỞI TẠO: Định nghĩa tên Job và Config
    JOB_NAME = 'load_delta'
    CONFIG_ID = None 
    
    # 2. GHI LOG START: Gọi vào Control DB để báo hiệu bắt đầu chạy
    start_result = log_start(JOB_NAME, CONFIG_ID)
    
    # 3. XỬ LÝ RUN ID: Tách lấy ID từ kết quả trả về
    if start_result and isinstance(start_result, tuple):
        run_id = start_result[0] # Lấy ID 
    else:
        run_id = start_result
        
    # 4. QUYẾT ĐỊNH: Có lấy được Run ID không?
    if not run_id:
        # [NHÁNH NO]: Dừng chương trình nếu không có ID
        print("Không khởi tạo được Run ID. Dừng chương trình.")
        return
    # 5. KẾT NỐI DB: Kết nối tới news_staging_db
    conn = connect_to_db("news_staging_db")

    # 6. QUYẾT ĐỊNH: Kết nối thành công không?
    if conn is None:
        # [NHÁNH NO]: Ghi log FAILED và Dừng
        log_end(run_id, "FAILED", 0, 0, "Connection Failed")
        return 

    try:
        cursor = conn.cursor()
        print(f"[RunID: {run_id}] Đang gọi Procedure...")
        
        # 7. GỌI PROCEDURE (load_to_staging_delta): Thực thi logic Incremental bên SQL
        cursor.callproc('load_to_staging_delta')
        
        result_value = None

        # 8. NHẬN KẾT QUẢ: Lấy output từ SQL (Số dòng hoặc Thông báo lỗi)
        for result in cursor.stored_results():
            row = result.fetchone()
            if row: result_value = row[0]

        # 9. COMMIT: Xác nhận giao dịch
        conn.commit()

        # 10. PHÂN LOẠI KẾT QUẢ: Kiểm tra kiểu dữ liệu trả về

        # 10.1: Nếu là Chuỗi (String) -> Có lỗi từ SQL
        if isinstance(result_value, str):
            err_msg = f"SQL Error: {result_value}"
            print(f"{err_msg}")
            log_end(run_id, "FAILED", 0, 0, err_msg)
        
        # 10.2: Nếu là Số (Int) -> Thành công
        elif isinstance(result_value, int): 
            print(f"Thành công! Load {result_value} dòng.")
            # Với bước này: extracted = loaded 
            log_end(run_id, "SUCCESS", records_extracted=result_value, records_loaded=result_value)
        
        # 10.3: Nếu là None -> Lỗi lạ
        else:
            msg = "Procedure returned None"
            print(f"{msg}")
            log_end(run_id, "FAILED", 0, 0, msg)

    except mysql.connector.Error as err:
        # 11. BẮT NGOẠI LỆ (Exception Handling): Nếu code Python bị lỗi
        err_msg = f"Python Error: {err}"
        print(f"{err_msg}")
        # 11.1 Ghi Log FAILED
        log_end(run_id, "FAILED", 0, 0, err_msg)
        
    finally:
        # 12. DỌN DẸP: Đóng kết nối Database
        if (conn.is_connected()):
            cursor.close()
            conn.close()
            print("Đã đóng kết nối.")

if __name__ == "__main__":
    run_incremental_etl()