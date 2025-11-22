import os
import mysql.connector
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore", category=DeprecationWarning) 

from utils.db_utils import connect_to_db

def run_incremental_etl():
    conn = connect_to_db("news_staging_db")

    if conn is None:
        return 

    try:
        cursor = conn.cursor()
        print("Đang gọi Procedure 'load_to_staging_delta'...")
        
        cursor.callproc('load_to_staging_delta')
        
        result_value = None
        
        # Lấy kết quả trả về
        for result in cursor.stored_results():
            row = result.fetchone()
            if row:
                result_value = row[0] # Lấy giá trị đầu tiên

        conn.commit()

        # --- KIỂM TRA KẾT QUẢ TRẢ VỀ ---
        
        # Trường hợp 1: Procedure trả về chữ (Thường là thông báo lỗi)
        if isinstance(result_value, str):
            print(f"Procedure gặp lỗi nội bộ trong MySQL. MySQL trả về: '{result_value}'")
        
        # Trường hợp 2: Procedure trả về số (Thành công)
        elif isinstance(result_value, int):
            if result_value > 0:
                print(f"Thành công! Đã load thêm {result_value} dòng mới vào bảng Delta.")
            else:
                print("Procedure chạy thành công nhưng không có dữ liệu mới nào.")
        
        # Trường hợp 3: Không trả về gì
        else:
            print("Procedure không trả về kết quả nào (None).")

    except mysql.connector.Error as err:
        print(f"Lỗi kết nối hoặc gọi Procedure: {err}")
    finally:
        if (conn.is_connected()):
            cursor.close()
            conn.close()
            print("Đã đóng kết nối.")

if __name__ == "__main__":
    run_incremental_etl()