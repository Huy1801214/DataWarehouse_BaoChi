# export_staging.py
import pandas as pd
from utils.db_utils import connect_to_db

def export():
    conn = connect_to_db("news_staging_db")
    if not conn: return
    
    print("📦 Đang xuất dữ liệu ra file CSV...")
    # Lấy dữ liệu
    df = pd.read_sql("SELECT * FROM staging_delta", conn)
    
    # Xuất file (Không header)
    df.to_csv("delta_data.csv", index=False, header=False)
    print(f"✅ Đã xuất {len(df)} dòng ra file: delta_data.csv")
    conn.close()

if __name__ == "__main__":
    export()