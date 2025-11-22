from utils.db_utils import connect_to_db
import uuid

class TransformLoader:
    """Chỉ gọi Stored Procedure transform."""

    def __init__(self, control_db="news_staging_db"):
        self.conn = connect_to_db(control_db)
        self.cursor = self.conn.cursor()
        self.run_id = str(uuid.uuid4())

    def run_transform(self):
        try:
            self.cursor.callproc("sp_transform_news_data", [self.run_id])
            self.conn.commit()
            print(f"[OK] Transform completed — run_id = {self.run_id}")
        except Exception as e:
            print(f"[ERROR] Transform failed: {e}")
            self.conn.rollback()

    def close(self):
        self.cursor.close()
        self.conn.close()
        
if __name__ == "__main__":
    loader = TransformLoader()
    try:
        loader.run_transform()
    finally:
        loader.close()