from dotenv import load_dotenv
import os
import mysql.connector

load_dotenv()

def get_db_connection():
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQL_HOST"),
            port=os.getenv("MYSQL_PORT"),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database="news_control_db"
        )
        print(f"[DB Utils] Kết nối thành công DB")
        return conn
    except Exception as e:
        print(f"[DB Utils] Lỗi kết nối đến {e}")
        return None

