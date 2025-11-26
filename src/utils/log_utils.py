from .db_utils import connect_to_db

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
def log_start(job_name: str, config_id: int = None):
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
            print(f"[LOG] Bắt đầu Job: {job_name} | Run ID: {run_id}")
            # Trả về run_id và conn (theo đúng format code cũ của bạn)
            return run_id, conn_control
        else:
            # NẾU SQL TRẢ VỀ NULL -> CÓ NGHĨA LÀ ĐANG BẬN
            print(f"HỆ THỐNG ĐANG BẬN: Có Job khác đang chạy (Start mà chưa End).")
            print("   -> Vui lòng chờ job cũ chạy xong.")
            return None, None

    except Exception as e:
        print(f"Lỗi khi ghi Log START: {e}")
        
    finally:
        if conn_control: conn_control.close()
        
    return None, None # Trả về None nếu có lỗi

# --- QUẢN LÝ GHI LOG END/FAIL ---
def log_end(run_id: str, status: str, records_extracted: int, records_loaded: int, error_message: str = None):
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