import os
import pandas as pd
import mysql.connector
import zlib
from dotenv import load_dotenv
import warnings

warnings.filterwarnings("ignore")
from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end 

def calculate_article_key(url):
    return zlib.crc32(url.encode('utf-8')) & 0xffffffff

def load_data_from_file():
    JOB_NAME = 'load_warehouse'
    CONFIG_ID = None 
    
    # 1. GHI LOG START
    start_result = log_start(JOB_NAME, CONFIG_ID)
    
    # Tách lấy ID ra khỏi Tuple
    if start_result and isinstance(start_result, tuple):
        run_id = start_result[0] # Lấy phần tử đầu tiên là ID
    else:
        run_id = start_result

    if not run_id:
        print("Không khởi tạo được Run ID.")
        return

    # Kiểm tra file CSV
    csv_file = "src/source/delta_data.csv"
    if not os.path.exists(csv_file):
        err_msg = f"Không tìm thấy file {csv_file}"
        print(f"{err_msg}")
        log_end(run_id, "FAILED", 0, 0, err_msg)
        return

    print(f"[BƯỚC 1] Đang đọc file CSV: {csv_file}...")
    col_names = ['article_url', 'source_name', 'category_name', 'author_name', 
                 'published_at', 'title', 'description', 'content', 
                 'scraped_at', 'run_id', 'tags']
    
    try:
        df = pd.read_csv(csv_file, header=None, names=col_names)
        df = df.where(pd.notnull(df), None)
        record_count = len(df)
    except Exception as e:
        err_msg = f"Lỗi đọc file CSV: {e}"
        print(f"{err_msg}")
        log_end(run_id, "FAILED", 0, 0, err_msg)
        return

    conn_dw = connect_to_db("news_warehouse_db")
    if not conn_dw: 
        log_end(run_id, "FAILED", 0, 0, "Connection Failed")
        return

    try:
        cursor = conn_dw.cursor()
        
        # 2. Đổ dữ liệu vào bảng BUFFER
        print(f"[BƯỚC 2] Nạp {record_count} dòng vào bảng 'buffer_delta'...")
        cursor.execute("TRUNCATE TABLE buffer_delta")
        
        insert_sql = """
        INSERT INTO buffer_delta (article_url, source_name, category_name, author_name, 
                  published_at, title, description, content, scraped_at, run_id, tags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        val = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_sql, val)
        conn_dw.commit()
        
        # 3. Gọi Procedure Merge
        print("[BƯỚC 3] Chạy Procedure Merge (Article)...")
        cursor.callproc('load_delta_to_warehouse')
        
        # Lấy kết quả
        loaded_count = 0
        for result in cursor.stored_results():
            row = result.fetchone()
            if row: loaded_count = row[0] # Số dòng thực sự insert/update vào kho
        
        conn_dw.commit()

        # Xử lý lỗi từ SQL (nếu có)
        if isinstance(loaded_count, str):
             raise Exception(f"SQL Error: {loaded_count}")

        print(f"   -> Kết quả: {loaded_count} bài báo thay đổi nội dung.")

        # 4. Xử lý TAGS 
        print("[BƯỚC 4] Kiểm tra và Cập nhật Tags (Tối ưu)...")
        cursor.execute("SELECT article_url, tags FROM buffer_delta WHERE tags IS NOT NULL AND tags != ''")
        delta_rows = cursor.fetchall()

        if delta_rows:
            cursor.execute("SELECT tag_name, tag_key FROM DimTag")
            tag_map = {row[0]: row[1] for row in cursor.fetchall()}
            insert_list = []
            delete_keys = []
            skipped_count = 0

            for row in delta_rows:
                url, raw_tags_str = row[0], row[1]
                art_key = calculate_article_key(url)
                
                new_tag_ids = set()
                tags_list = [t.strip() for t in raw_tags_str.split(',')]
                for t_name in tags_list:
                    if t_name in tag_map: new_tag_ids.add(tag_map[t_name])
                
                cursor.execute("SELECT tag_key FROM Bridge_Article_Tag WHERE article_key = %s", (art_key,))
                current_tag_ids = set(r[0] for r in cursor.fetchall())
                
                if new_tag_ids == current_tag_ids:
                    skipped_count += 1
                    continue
                
                delete_keys.append(art_key)
                for t_id in new_tag_ids: insert_list.append((art_key, t_id))

            if delete_keys:
                format_strings = ','.join(['%s'] * len(delete_keys))
                cursor.execute(f"DELETE FROM Bridge_Article_Tag WHERE article_key IN ({format_strings})", tuple(delete_keys))
            if insert_list:
                cursor.executemany("INSERT IGNORE INTO Bridge_Article_Tag (article_key, tag_key) VALUES (%s, %s)", insert_list)
            conn_dw.commit()
            print(f"Tags: Cập nhật {len(delete_keys)}, Bỏ qua {skipped_count}.")

        # --- GHI LOG THÀNH CÔNG ---
        # extracted: số dòng đọc từ file CSV (record_count)
        # loaded: số dòng thực sự thay đổi trong DWH (loaded_count)
        log_end(run_id, "SUCCESS", records_extracted=record_count, records_loaded=loaded_count)
        print("QUY TRÌNH HOÀN TẤT!")

    except Exception as e:
        err_msg = str(e)
        print(f"Lỗi Warehouse ETL: {err_msg}")
        log_end(run_id, "FAILED", 0, 0, err_msg)
        
    finally:
        if conn_dw.is_connected(): 
            cursor.close()
            conn_dw.close()
            print("Đã đóng kết nối.")

if __name__ == "__main__":
    load_data_from_file()