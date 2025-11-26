import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv()

def get_control_db_engine():
    try:
        host = os.getenv("MYSQL_HOST")
        port = os.getenv("MYSQL_PORT", "3306")
        user = os.getenv("MYSQL_USER")
        password = os.getenv("MYSQL_PASSWORD")
        database = "news_control_db" 

        url = f"mysql+mysqlconnector://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(url)
        print("[Info] Đã tạo Engine kết nối CSDL Control.")
        return engine
    except Exception as e:
        print(f"[Lỗi] Không thể tạo Engine: {e}")
        return None

def load_config_data(csv_file_path):
    # 1. Kiểm tra file tồn tại
    if not os.path.exists(csv_file_path):
        print(f"[Lỗi] Không tìm thấy file: {csv_file_path}")
        return

    # 2. Kết nối CSDL
    engine = get_control_db_engine()
    if not engine:
        return

    try:
        # 3. Đọc CSV bằng Pandas
        print(f"[Info] Đang đọc file: {csv_file_path}")
        df = pd.read_csv(csv_file_path)
        
        print(f"[Info] Dữ liệu đọc được ({len(df)} dòng):")
        print(df.head())

        # 4. Xử lý dữ liệu 
        if 'date_dim' not in df.columns:
            df['date_dim'] = pd.to_datetime('today').date()

        # 5. Nạp vào MySQL
        table_name = 'config_table' 
        
        with engine.begin() as connection:   
            print(f"[Info] Đang nạp dữ liệu vào bảng {table_name}...")
            df.to_sql(table_name, con=connection, if_exists='append', index=False)
            
        print("\n[Thành công] Đã nạp xong dữ liệu vào bảng Config_table.")

    except Exception as e:
        print(f"[Lỗi Nghiêm Trọng] Quá trình nạp thất bại: {e}")

if __name__ == "__main__":
    BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    csv_path = os.path.join(BASE_DIR, "source", "control", "config_data.csv")
    load_config_data(csv_path)