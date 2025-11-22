import os
import mysql.connector

def connect_to_db(db_name):
    try:
        conn = mysql.connector.connect(
            host=os.getenv("MYSQLHOST", "shuttle.proxy.rlwy.net"),
            port=int(os.getenv("MYSQLPORT", 3306)),
            user=os.getenv("MYSQLUSER", "root"),
            password=os.getenv("MYSQLPASSWORD"),
            database=db_name
        )
        print(f"Kết nối {db_name} thành công!")
        return conn
    except Exception as e:
        print("Lỗi kết nối:", e)
        return None


connect_to_db("news_staging_db")