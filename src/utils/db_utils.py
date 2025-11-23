from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

def connect_to_db(db_name):
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=db_name
        )
        print(f"Kết nối {db_name} thành công!")
        return conn
    except mysql.connector.Error as e:
        print(f"Lỗi khi kết nối {db_name}: {e}")
        return None
    
# Các hàm ghi log (Sẽ gọi SPs)
def execute_sp(conn, procname, args):
    """Hàm chung để gọi Stored Procedure."""
    cursor = conn.cursor()
    result_args = cursor.callproc(procname, args)
    conn.commit()
    # Nếu là SP_Start_Log, lấy run_id
    if procname == 'SP_Start_Log':
        run_id = result_args[2]
        return run_id
    return None


# --- QUẢN LÝ GHI LOG START ---
def log_startg(job_name: str, config_id: int = None):
    """
    Ghi sự kiện START vào Control DB và trả về run_id.
    """
    conn_control = connect_to_db("news_control_db")
    if not conn_control:
        print("Lỗi: Không thể kết nối DB Control để ghi Log START.")
        return None, None
    
    try:
        # 1. Chuẩn bị tham số (config_id, job_name, OUT variable name)
        args = [config_id, job_name, None] 
        
        # 2. Thực thi SP: execute_sp sẽ tự động commit và trả về run_id
        run_id = execute_sp(conn_control, 'SP_Start_Log', args)
        
        if run_id:
            print(f"Ghi Log START thành công. RUN_ID: {run_id}")
            return run_id, conn_control

    except Exception as e:
        print(f"Lỗi khi ghi Log START: {e}")
        
    finally:
        if conn_control: conn_control.close()
        
    return None, None # Trả về None nếu có lỗi

# --- QUẢN LÝ GHI LOG END/FAIL ---
def log_endg(run_id: str, status: str, records_extracted: int, records_loaded: int, error_message: str = None):
    """
    Ghi sự kiện END (SUCCESS/FAIL) vào Control DB.
    Sử dụng SP_End_Log.
    """
    conn_control = connect_to_db("news_control_db")
    if not conn_control:
        print("Lỗi: Không thể kết nối DB Control để ghi Log END/FAIL.")
        return
        
    try:
        # 1. Chuẩn bị tham số đầu vào (không có tham số OUT)
        args = [
            run_id, 
            status, 
            records_extracted, 
            records_loaded, 
            error_message
        ]
        
        # 2. Thực thi SP:
        execute_sp(conn_control, 'SP_End_Log', args)
        
        print(f"Ghi Log END thành công. RUN_ID: {run_id}, Status: {status}")

    except Exception as e:
        print(f"Lỗi khi ghi Log END: {e}")
        
    finally:
        if conn_control: conn_control.close()
