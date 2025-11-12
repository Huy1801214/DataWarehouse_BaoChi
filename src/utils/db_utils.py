from datetime import date, datetime
import os
import mysql.connector
from dotenv import load_dotenv

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

# --- Chức năng Ghi Log ---

def start_job_log(job_name, config_id=None):
    """Ghi log bắt đầu job và trả về run_id."""
    conn = connect_to_db("news_control_db")
    if not conn: return None, None
    
    start_time = datetime.now()
    try:
        cursor = conn.cursor()
        query = """
        INSERT INTO logging_table 
        (config_id, job_name, start_time, status, date_dim)
        VALUES (%s, %s, %s, %s, %s)
        """
        cursor.execute(query, (config_id, job_name, start_time, "RUNNING", date.today()))
        conn.commit()
        run_id = cursor.lastrowid
        return run_id, conn # Trả về run_id và kết nối để update sau
    except Exception as e:
        print(f"Lỗi khi ghi start_log: {e}")
        if conn: conn.close()
        return None, None

def end_job_log(run_id, status, records_extracted=0, records_loaded=0, error_message=None):
    """Cập nhật log khi job kết thúc."""
    conn = connect_to_db("news_control_db")
    if not conn: return
    
    end_time = datetime.now()
    try:
        cursor = conn.cursor()
        query = """
        UPDATE logging_table SET 
        end_time = %s, status = %s, records_extracted = %s, 
        records_loaded = %s, error_message = %s
        WHERE run_id = %s
        """
        cursor.execute(query, (end_time, status, records_extracted, records_loaded, error_message, run_id))
        
        # Cập nhật next_run_at nếu job Extract (config_id tồn tại)
        if status == "SUCCESS" and records_extracted > 0:
            cursor.execute("SELECT config_id FROM logging_table WHERE run_id = %s", (run_id,))
            config_id = cursor.fetchone()[0] if cursor.rowcount > 0 and cursor.fetchone() else None
            
            if config_id:
                 # Logic giả định: next_run_at = start_time + 1 ngày (dựa trên crawl_interval 'daily')
                 new_next_run = end_time.replace(hour=end_time.hour + 24, minute=0, second=0, microsecond=0)
                 cursor.execute("UPDATE config_table SET last_run_at = %s, next_run_at = %s, updated_at = %s WHERE config_id = %s", 
                                (end_time, new_next_run, end_time, config_id))

        conn.commit()
    except Exception as e:
        print(f"Lỗi khi cập nhật end_log: {e}")
    finally:
        if conn: conn.close()
        
def load_config(source_name=None):
    """Tải cấu hình từ config_table."""
    conn = connect_to_db("news_control_db")
    if not conn: return None

    try:
        cursor = conn.cursor(dictionary=True)
        query = "SELECT * FROM config_table WHERE active = 1"
        if source_name:
            query += " AND source_name = %s"
            cursor.execute(query, (source_name,))
        else:
            cursor.execute(query)
            
        return cursor.fetchall()
    except Exception as e:
        print(f"Lỗi khi tải config: {e}")
        return None
    finally:
        if conn: conn.close()

def log_error(run_id, error_message):
    """Cập nhật trạng thái FAIL và ghi lỗi."""
    end_job_log(run_id, "FAIL", error_message=error_message)