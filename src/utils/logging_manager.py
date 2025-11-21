import mysql.connector
from datetime import datetime

class LoggingManager:
    def __init__(self, conn, run_id):
        self.conn = conn
        self.run_id = run_id

    def start(self, config_id, job_name):
        """
        Bắt đầu một job: Tạo một dòng log mới với trạng thái RUNNING.
        """
        try:
            cursor = self.conn.cursor()
            check_query = "SELECT 1 FROM logging_table WHERE run_id = %s AND config_id = %s AND job_name = %s"
            cursor.execute(check_query, (self.run_id, config_id, job_name))
            exists = cursor.fetchone()

            if not exists:
                query = """
                    INSERT INTO logging_table 
                        (run_id, config_id, job_name, start_time, status, date_dim)
                    VALUES (%s, %s, %s, NOW(), 'RUNNING', CURRENT_DATE)
                """
                
                cursor.execute(query, (self.run_id, config_id, job_name))
                self.conn.commit()
                print(f"[LOG] Đã khởi tạo log cho job: {job_name} (Status: RUNNING)")
            else:
                print(f"[LOG] Job {job_name} đã có log đang chạy, bỏ qua tạo mới.")
            
            cursor.close()
        except Exception as e:
            print(f"[ERROR] Không thể ghi log start: {e}")

    def end(self, config_id, job_name, status, records_extracted=0, error_message=None):
        """
        Kết thúc một job: Cập nhật trạng thái (SUCCESS/FAILED), thời gian kết thúc và số lượng bản ghi.
        """
        try:
            cursor = self.conn.cursor()
            query = """
                UPDATE logging_table
                SET end_time = NOW(),
                    status = %s,
                    records_extracted = %s,
                    error_message = %s
                WHERE run_id = %s AND config_id = %s AND job_name = %s
            """
            cursor.execute(query, (status, records_extracted, error_message, self.run_id, config_id, job_name))
            self.conn.commit()
            
            if cursor.rowcount == 0:
                print(f"[WARNING] Không tìm thấy dòng log để cập nhật cho job {job_name} (run_id: {self.run_id})")
            else:
                print(f"[LOG] Đã cập nhật trạng thái job: {job_name} -> {status}")
                
            cursor.close()
        except Exception as e:
            print(f"[ERROR] Không thể ghi log end: {e}")