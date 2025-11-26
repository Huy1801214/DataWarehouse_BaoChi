import os
import pandas as pd
import mysql.connector
import zlib
from dotenv import load_dotenv
import warnings
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
warnings.filterwarnings("ignore")
from utils.db_utils import connect_to_db
from utils.log_utils import log_start, log_end 

def calculate_article_key(url):
    return zlib.crc32(url.encode('utf-8')) & 0xffffffff

def load_data_from_file():
    # 1. KHỞI TẠO: Định nghĩa Job name
    JOB_NAME = 'load_warehouse'
    CONFIG_ID = None 
    
    # 2. GHI LOG START: Gọi vào Control DB để báo hiệu bắt đầu chạy
    start_result = log_start(JOB_NAME, CONFIG_ID)
    
    # 3. XỬ LÝ RUN ID: Tách lấy ID từ kết quả trả về
    if start_result and isinstance(start_result, tuple):
        run_id = start_result[0] 
    else:
        run_id = start_result

    # 4. QUYẾT ĐỊNH: Có lấy được Run ID không?
    if not run_id:
        # [NHÁNH NO]: Dừng chương trình nếu không có ID
        print("Không khởi tạo được Run ID.")
        return

    # 5. KIỂM TRA FILE: File CSV có tồn tại không?
    csv_file = "source/delta_data.csv"
    if not os.path.exists(csv_file):
        err_msg = f"Không tìm thấy file {csv_file}"
        print(f"{err_msg}")
        # [NHÁNH NO]: Ghi Log Thất Bại & Kết thúc
        log_end(run_id, "FAILED", 0, 0, err_msg)
        return

    print(f"[BƯỚC 1] Đang đọc file CSV: {csv_file}...")
    col_names = ['article_url', 'source_name', 'category_name', 'author_name', 
                 'published_at', 'title', 'description', 'content', 
                 'scraped_at', 'run_id', 'tags']
    
    try:
        # 6. ĐỌC FILE: Load CSV vào Pandas DataFrame (RAM)
        df = pd.read_csv(csv_file, header=None, names=col_names)
        df = df.where(pd.notnull(df), None)
        record_count = len(df)
    except Exception as e:
        err_msg = f"Lỗi đọc file CSV: {e}"
        print(f"{err_msg}")
        # [NHÁNH YES]: Nếu có lỗi: Ghi Log Lỗi đọc file & Kết thúc
        log_end(run_id, "FAILED", 0, 0, err_msg)
        return

    # 7. KẾT NỐI DB: Kết nối tới news_warehouse_db
    conn_dw = connect_to_db("news_warehouse_db")

    # 8. QUYẾT ĐỊNH: Kết nối thành công không?
    if not conn_dw: 
        # [NHÁNH NO]: Ghi log FAILED và Dừng
        log_end(run_id, "FAILED", 0, 0, "Connection Failed")
        return

    try:
        cursor = conn_dw.cursor()
        
        # 9. NẠP BUFFER: Xóa sạch bảng tạm và đổ dữ liệu từ RAM vào
        print(f"[BƯỚC 2] Nạp {record_count} dòng vào bảng 'buffer_delta'...")
        cursor.execute("TRUNCATE TABLE buffer_delta")
        
        insert_sql = """
        INSERT INTO buffer_delta (article_url, source_name, category_name, author_name, 
                  published_at, title, description, content, scraped_at, run_id, tags)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        val = [tuple(x) for x in df.to_numpy()]
        cursor.executemany(insert_sql, val)

        # 10. COMMIT BUFFER: Lưu dữ liệu bảng tạm
        conn_dw.commit()
        
        # 11. GỌI PROCEDURE: Chuyển logic xử lý Article cho SQL
        print("[BƯỚC 3] Chạy Procedure Merge (Article)...")
        cursor.callproc('load_delta_to_warehouse')
        
        # 12. NHẬN KẾT QUẢ SQL: Số dòng thay đổi
        loaded_count = 0
        for result in cursor.stored_results():
            row = result.fetchone()
            if row: loaded_count = row[0] # Số dòng thực sự insert/update vào kho
        
        conn_dw.commit()

        # 13. KIỂM TRA LỖI SQL 
        if isinstance(loaded_count, str):
             raise Exception(f"SQL Error: {loaded_count}")

        print(f"   -> Kết quả: {loaded_count} bài báo thay đổi nội dung.")

        # 14. CHUẨN BỊ XỬ LÝ TAGS: Query lấy danh sách bài báo và tags từ bảng Buffer
        print("[BƯỚC 4] Kiểm tra và Cập nhật Tags ...")
        cursor.execute("SELECT article_url, tags FROM buffer_delta WHERE tags IS NOT NULL AND tags != ''")
        delta_rows = cursor.fetchall()

        if delta_rows:
            # 15. TẢI MAP TAGS: Lấy danh sách Tag ID từ DB lên RAM để tra cứu
            cursor.execute("SELECT tag_name, tag_key FROM DimTag")
            tag_map = {row[0]: row[1] for row in cursor.fetchall()}
            insert_list = []
            delete_keys = []
            skipped_count = 0

            # 16. VÒNG LẶP (Set Comparison Loop)
            for row in delta_rows:
                url, raw_tags_str = row[0], row[1]
                art_key = calculate_article_key(url)
                
                # 16.1. Tạo Set Tags Mới
                new_tag_ids = set()
                tags_list = [t.strip() for t in raw_tags_str.split(',')]
                for t_name in tags_list:
                    if t_name in tag_map: new_tag_ids.add(tag_map[t_name])
                
                # 16.2. Query Set Tags Cũ từ Bridge
                cursor.execute("SELECT tag_key FROM Bridge_Article_Tag WHERE article_key = %s", (art_key,))
                current_tag_ids = set(r[0] for r in cursor.fetchall())
                
                # 16.3. So sánh 2 Set
                if new_tag_ids == current_tag_ids:
                    skipped_count += 1
                    continue
                
                # 16.4. Khác nhau -> Thêm vào danh sách xử lý
                delete_keys.append(art_key)
                for t_id in new_tag_ids: insert_list.append((art_key, t_id))

            # 17. CẬP NHẬT Bridge_Article_Tag: Thực hiện xóa và thêm mới
            if delete_keys:
                format_strings = ','.join(['%s'] * len(delete_keys))
                cursor.execute(f"DELETE FROM Bridge_Article_Tag WHERE article_key IN ({format_strings})", tuple(delete_keys))
            if insert_list:
                cursor.executemany("INSERT IGNORE INTO Bridge_Article_Tag (article_key, tag_key) VALUES (%s, %s)", insert_list)

            # 18. COMMIT TAGS
            conn_dw.commit()
            print(f"Tags: Cập nhật {len(delete_keys)}, Bỏ qua {skipped_count}.")

        # 19. GHI LOG THÀNH CÔNG (Log End - Success)
        # extracted: số dòng đọc từ file CSV (record_count)
        # loaded: số dòng thực sự thay đổi trong DWH (loaded_count)
        log_end(run_id, "SUCCESS", records_extracted=record_count, records_loaded=loaded_count)
        print("QUY TRÌNH HOÀN TẤT!")

    except Exception as e:
        # 20. Exception Handling: Ghi log FAILED
        err_msg = str(e)
        print(f"Lỗi Warehouse ETL: {err_msg}")
        # 20.1. Ghi log Failed
        log_end(run_id, "FAILED", 0, 0, err_msg)
        
    finally:
        # 21. DỌN DẸP: Đóng kết nối Database
        if conn_dw.is_connected(): 
            cursor.close()
            conn_dw.close()
            print("Đã đóng kết nối.")

if __name__ == "__main__":
    load_data_from_file()