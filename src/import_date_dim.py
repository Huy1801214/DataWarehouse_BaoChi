import pandas as pd
import mysql.connector
from dotenv import load_dotenv
import os
from utils.db_utils import connect_to_db

def import_date_dim():
    csv_file = "date_dim.csv"
    
    if not os.path.exists(csv_file):
        print(f"Không tìm thấy file {csv_file}")
        return

    print("📖 Đang đọc file CSV...")
    # Đọc file 
    df = pd.read_csv(csv_file, header=None)
    
    # Chọn đúng các cột cần thiết dựa trên dữ liệu thực tế:
    # Cột 0: date_key (1)
    # Cột 1: full_date (2005-01-01)
    # Cột 4: day_of_week (Saturday)
    # Cột 5: month_name (January)
    # Cột 6: year (2005)
    # Cột 8: day_of_month (1)
    # Cột 18: holiday (Non-Holiday)
    # Cột 19: day_type (Weekend)
    
    df_filtered = df.iloc[:, [0, 1, 4, 5, 6, 8, 18, 19]]
    
    # Convert NaN thành None để tránh lỗi DB
    df_filtered = df_filtered.where(pd.notnull(df_filtered), None)
    
    print(f"Tìm thấy {len(df_filtered)} dòng dữ liệu.")

    conn = connect_to_db("news_warehouse_db")
    if conn is None:
        return

    try:
        cursor = conn.cursor()
        
        # 1. Xóa dữ liệu cũ để tránh trùng lặp
        print("Đang làm sạch bảng DimDate...")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("TRUNCATE TABLE DimDate;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        
        # 2. Import dữ liệu mới
        print("Đang nạp dữ liệu vào Database...")
        sql = """
        INSERT INTO DimDate 
        (date_key, full_date, day_of_week, month_name, year, day_of_month, holiday, day_type)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Chuyển đổi sang list tuple để insert nhanh
        values = [tuple(x) for x in df_filtered.to_numpy()]
        
        cursor.executemany(sql, values)
        conn.commit()
        
        print(f"THÀNH CÔNG! Đã import {cursor.rowcount} dòng vào bảng DimDate.")
        
    except mysql.connector.Error as err:
        print(f"Lỗi Import: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()
            print("Đã đóng kết nối.")

if __name__ == "__main__":
    import_date_dim()