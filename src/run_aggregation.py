import mysql.connector
import o

# KHÔNG BAO GIỜ hardcode (viết thẳng) mật khẩu vào code.
# Hãy dùng Biến Môi trường (Environment Variables).
# Railway tự động quản lý việc này.
#
# Lấy các biến từ Railway (an toàn)
DB_HOST = os.getenv("MYSQLHOST", "shuttle.proxy.rlwy.net")
DB_PORT = int(os.getenv("MYSQLPORT", 41466))
DB_USER = os.getenv("MYSQLUSER", "root")
DB_PASSWORD = os.getenv("MYSQLPASSWORD") # Lấy mật khẩu từ Railway
DB_NAME = os.getenv("MYSQLDATABASE") # Lấy tên database từ Railway

def connect_to_db():
    if not DB_PASSWORD or not DB_NAME:
        print("Lỗi: Vui lòng thiết lập biến môi trường MYSQLPASSWORD và MYSQLDATABASE")
        return None
        
    try:
        conn = mysql.connector.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            database=DB_NAME
        )
        print(f"Kết nối {DB_NAME} thành công!")
        return conn
    except Exception as e:
        print(f"Lỗi khi kết nối {DB_NAME}: {e}")
        return None

def run_aggregation(conn):
    """
    Hàm chính để chạy TRUNCATE và INSERT...SELECT...
    """
    cursor = None
    try:
        cursor = conn.cursor()
        
        # --- Câu lệnh 1: Làm rỗng bảng aggregate ---
        print("Đang làm rỗng bảng tổng hợp (TRUNCATE)...")
        cursor.execute("TRUNCATE TABLE Agg_Mart_Top_Trends;")
        print("Bảng tổng hợp đã được làm rỗng.")

        # --- Câu lệnh 2: Chạy truy vấn tổng hợp và nạp dữ liệu ---
        print("Đang chạy truy vấn tổng hợp (INSERT ... SELECT ...)")
        
        aggregation_query = """
        INSERT INTO Agg_Mart_Top_Trends 
            (Year, Month_Name, Topic_Name, TenTrangBao, SoLanXuatHien)
        SELECT
            d.Year,
            d.Month_Name,
            t.Topic_Name,
            a.TenTrangBao,
            SUM(f.Article_Topic_Count) AS SoLanXuatHien
        FROM Fact_Article_Topics f
        JOIN Dim_Date d ON f.Date_Key = d.Date_Key
        JOIN Dim_Article a ON f.Article_Key = a.Article_Key
        JOIN Dim_Topic t ON f.Topic_Key = t.Topic_Key
        WHERE
            -- Rất quan trọng: Chỉ tổng hợp trên phiên bản MỚI NHẤT của bài báo
            a.Row_Is_Current = 'Yes' 
        GROUP BY
            d.Year,
            d.Month_Name,
            t.Topic_Name,
            a.TenTrangBao;
        """
        
        cursor.execute(aggregation_query)
        
        # --- Quan trọng: Lưu các thay đổi vào DB ---
        conn.commit()
        
        print(f"Hoàn thành! Đã nạp {cursor.rowcount} dòng vào bảng Agg_Mart_Top_Trends.")

    except Exception as e:
        print(f"Lỗi nghiêm trọng khi chạy aggregation: {e}")
        # Nếu có lỗi, rollback (phục hồi) lại mọi thay đổi
        if conn:
            conn.rollback()
            print("Đã rollback (phục hồi) các thay đổi.")
    finally:
        # Luôn đóng cursor khi kết thúc
        if cursor:
            cursor.close()

# --- Hàm chính để chạy script ---
if __name__ == "__main__":
    db_conn = connect_to_db()
    
    if db_conn:
        run_aggregation(db_conn)
        # Đóng kết nối khi xong
        db_conn.close()
        print("Đã đóng kết nối CSDL.")