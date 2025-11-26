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
            database=db_name,
            ssl_disabled=True
        )
        print(f"Kết nối {db_name} thành công!")
        return conn
    except mysql.connector.Error as e:
        print(f"Lỗi khi kết nối {db_name}: {e}")
        return None

