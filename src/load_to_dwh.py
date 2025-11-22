import os
import mysql.connector
from dotenv import load_dotenv
import zlib
import warnings

warnings.filterwarnings("ignore")
from utils.db_utils import connect_to_db

def calculate_article_key(url):
    """Tính CRC32 giống MySQL để ra article_key"""
    return zlib.crc32(url.encode('utf-8')) & 0xffffffff

def run_warehouse_etl():
    conn_dw = connect_to_db("news_warehouse_db")
    conn_staging = connect_to_db("news_staging_db")
    
    if not conn_dw or not conn_staging: return

    try:
        cursor_dw = conn_dw.cursor()
        cursor_staging = conn_staging.cursor(dictionary=True)

        # --- BƯỚC 1: Chạy Procedure SQL (Load Article) ---
        print("BƯỚC 1: Load DimArticle...")
        cursor_dw.callproc('load_delta_to_warehouse')
        conn_dw.commit() 
        # (Lưu ý: Procedure đã lọc Hash nội dung, nên chỉ những bài thực sự thay đổi nội dung mới được update)
        
        # --- BƯỚC 2: Xử lý Bridge Tags (TỐI ƯU SET COMPARISON) ---
        print("BƯỚC 2: Kiểm tra và Cập nhật Tags (Chế độ Tối ưu)...")
        
        # Lấy bài báo trong Delta (Tất cả bài vừa cào về)
        cursor_staging.execute("SELECT article_url, tags FROM staging_delta WHERE tags IS NOT NULL AND tags != ''")
        delta_rows = cursor_staging.fetchall()

        if delta_rows:
            # Lấy map Tag_Name -> Tag_ID từ Warehouse
            cursor_dw.execute("SELECT tag_name, tag_key FROM DimTag")
            tag_map = {row[0]: row[1] for row in cursor_dw.fetchall()}
            
            insert_list = []
            delete_keys = []
            skipped_count = 0 # Đếm số bài được bỏ qua (Tối ưu)

            for row in delta_rows:
                url = row['article_url']
                raw_tags_str = row['tags']
                
                art_key = calculate_article_key(url)
                
                # --- [LOGIC TỐI ƯU BẮT ĐẦU TỪ ĐÂY] ---

                # 1. Xác định TẬP HỢP TAGS MỚI (New Set)
                # Chuyển chuỗi "A, B" -> Tập hợp ID {1, 2}
                new_tag_ids = set()
                tags_list = [t.strip() for t in raw_tags_str.split(',')]
                for t_name in tags_list:
                    if t_name in tag_map:
                        new_tag_ids.add(tag_map[t_name])
                
                # 2. Xác định TẬP HỢP TAGS CŨ trong kho (Old Set)
                # Query xem bài này hiện tại đang có những tag ID nào
                cursor_dw.execute("SELECT tag_key FROM Bridge_Article_Tag WHERE article_key = %s", (art_key,))
                current_tag_ids = set(r[0] for r in cursor_dw.fetchall())
                
                # 3. SO SÁNH HAI TẬP HỢP
                if new_tag_ids == current_tag_ids:
                    # TRƯỜNG HỢP A: Giống hệt nhau -> Bỏ qua!
                    skipped_count += 1
                    continue # Nhảy sang bài tiếp theo, không làm gì bài này cả
                
                # TRƯỜNG HỢP B: Khác nhau -> Đưa vào danh sách cần xử lý
                # (Có thể là thêm tag mới, bớt tag cũ, hoặc đổi tag khác)
                delete_keys.append(art_key) # Đánh dấu để xóa cũ
                for t_id in new_tag_ids:
                    insert_list.append((art_key, t_id)) # Chuẩn bị thêm mới

                # --- [LOGIC TỐI ƯU KẾT THÚC] ---

            # A. Thực hiện Xóa (Chỉ xóa những bài có thay đổi)
            if delete_keys:
                print(f"   -> Phát hiện {len(delete_keys)} bài báo thay đổi Tags.")
                format_strings = ','.join(['%s'] * len(delete_keys))
                cursor_dw.execute(f"DELETE FROM Bridge_Article_Tag WHERE article_key IN ({format_strings})", tuple(delete_keys))
            
            # B. Thực hiện Thêm mới
            if insert_list:
                cursor_dw.executemany("INSERT IGNORE INTO Bridge_Article_Tag (article_key, tag_key) VALUES (%s, %s)", insert_list)
            
            conn_dw.commit()
            
            print(f"HOÀN TẤT: Đã cập nhật {len(delete_keys)} bài. Bỏ qua {skipped_count} bài (Tags không đổi).")
        else:
            print("Không có dữ liệu Tags để xử lý.")

    except mysql.connector.Error as err:
        print(f"Lỗi: {err}")
    finally:
        if conn_dw.is_connected(): conn_dw.close()
        if conn_staging.is_connected(): conn_staging.close()
        print("Đã đóng kết nối.")

if __name__ == "__main__":
    run_warehouse_etl()