import mysql.connector
from datetime import datetime, date
import uuid
from utils.db_utils import connect_to_db


def load_to_staging_delta():
    conn = connect_to_db("news_staging_db")
    cursor = conn.cursor(dictionary=True)

    # lấy config
    cursor.execute("""
        SELECT * FROM config WHERE active = TRUE LIMIT 1;
    """)
    config = cursor.fetchone()
    if not config:
        print("No active config found.")
        return

    config_id = config["config_id"]
    run_id = str(uuid.uuid4())
    start_time = datetime.now()

    try:
        # lấy last_load_time từ etl_metadata
        cursor.execute("""
            SELECT last_load_time 
            FROM etl_metadata 
            WHERE process_name = 'load_to_staging_delta'
        """)
        result = cursor.fetchone()
        last_load_time = result["last_load_time"] if result else datetime(2000, 1, 1)

        # lấy data mới hơn last_load_time 
        cursor.execute("""
            SELECT * 
            FROM transformed_temp
            WHERE published_at > %s
        """, (last_load_time,))
        new_records = cursor.fetchall()

        print(f"Found {len(new_records)} new/updated records since {last_load_time}")

        if not new_records:
            print("No new records found. Nothing to load.")
            return

        # ghi mới vào staging_delta 
        insert_sql = """
            INSERT INTO staging_delta (
                article_url, source_name, category_name, author_name,
                published_at, title, description, word_count,
                tags, sentiment_score, run_id, datadim, is_new, is_updated, loaded_at
            ) VALUES (
                %(article_url)s, %(source_name)s, %(category_name)s, %(author_name)s,
                %(published_at)s, %(title)s, %(description)s, %(word_count)s,
                %(tags)s, %(sentiment_score)s, %(run_id)s, %(datadim)s, TRUE, FALSE, NOW()
            )
            ON DUPLICATE KEY UPDATE
                is_updated = TRUE,
                loaded_at = NOW()
        """

        for record in new_records:
            record["run_id"] = run_id
            record["datadim"] = date.today()  
            cursor.execute(insert_sql, record)

        records_loaded = len(new_records)
        conn.commit()

        # update etl_metadata
        cursor.execute("""
            INSERT INTO etl_metadata (process_name, last_load_time)
            VALUES ('load_to_staging_delta', %s)
            ON DUPLICATE KEY UPDATE last_load_time = VALUES(last_load_time)
        """, (datetime.now(),))
        conn.commit()

        # ghi log
        end_time = datetime.now()
        cursor.execute("""
            INSERT INTO logging (
                run_id, config_id, job_name, start_time, end_time,
                status, records_extracted, records_loaded, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id, config_id, 'load_to_staging_delta', start_time, end_time,
            'SUCCESS', records_loaded, records_loaded, None
        ))
        conn.commit()

        print(f"Load to staging_delta completed successfully. {records_loaded} records loaded.")

    except Exception as e:
        conn.rollback()
        error_msg = str(e)

        # ghi log lỗi
        cursor.execute("""
            INSERT INTO logging (
                run_id, config_id, job_name, start_time, end_time,
                status, records_extracted, records_loaded, error_message
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id, config_id, 'load_to_staging_delta', start_time, datetime.now(),
            'FAILED', 0, 0, error_msg
        ))
        conn.commit()

        print("Error:", error_msg)

    finally:
        cursor.close()
        conn.close()


if __name__ == "__main__":
    load_to_staging_delta()
