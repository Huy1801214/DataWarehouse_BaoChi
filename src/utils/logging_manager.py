import mysql.connector
from datetime import datetime

class LoggingManager:
    def __init__(self, conn, run_id):
        self.conn = conn
        self.run_id = run_id

    def start(self, config_id, job_name):
        """Ghi log khi bắt đầu job"""
        try:
            cursor = self.conn.cursor()
            query = """
                INSERT INTO logging_table 
                    (run_id, config_id, job_name, start_time, status, date_dim)
                VALUES (%s, %s, %s, NOW(), 'RUNNING', CURRENT_DATE)
            """
            cursor.execute(query, (self.run_id, config_id, job_name))
            self.conn.commit()
            cursor.close()

        except Exception as e:
            print("[Logging ERROR] Không thể ghi start log:", e)

    def end(self, status, records_extracted, error_message=None):
        """Ghi log khi kết thúc job"""
        try:
            cursor = self.conn.cursor()
            query = """
                UPDATE logging_table
                SET end_time = NOW(),
                    status = %s,
                    records_extracted = %s,
                    error_message = %s
                WHERE run_id = %s
            """
            cursor.execute(query, (status, records_extracted, error_message, self.run_id))
            self.conn.commit()
            cursor.close()

        except Exception as e:
            print("[Logging ERROR] Không thể ghi end log:", e)
