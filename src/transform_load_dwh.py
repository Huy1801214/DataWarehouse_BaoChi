import datetime
import uuid
from utils.db_utils import connect_to_db

def clean_text(text):
    if not text:
        return None
    return text.strip().replace("\n", " ").replace("\r", " ")

def main():
    # connect db
    conn_stg = connect_to_db("news_staging_db")
    conn_dw = connect_to_db("news_warehouse_db")

    if not conn_stg or not conn_dw:
        print("Không thể kết nối đến database.")
        return

    cur_stg = conn_stg.cursor(dictionary=True)
    cur_dw = conn_dw.cursor()

    run_id = str(uuid.uuid4())
    print(f"ETL Run ID: {run_id}")

    # lấy dữ liệu từ stagging
    cur_stg.execute("""
        SELECT article_url, source_name_raw, category_raw, author_raw,
               title_raw, summary_raw, content_raw, scraped_at
        FROM staging_temp_table
    """)
    rows = cur_stg.fetchall()
    print(f"Đã lấy {len(rows)} bản ghi từ staging_temp_table")

    # transform 
    transformed = []
    for r in rows:
        transformed.append({
            "article_url": r["article_url"],
            "source_name": clean_text(r["source_name_raw"]),
            "category_name": clean_text(r["category_raw"]),
            "author_name": clean_text(r["author_raw"]),
            "title": clean_text(r["title_raw"]),
            "description": clean_text(r["summary_raw"]),
            "fullcontent": clean_text(r["content_raw"]),
            "published_at": r["scraped_at"],
            "word_count": len(r["content_raw"].split()) if r["content_raw"] else 0,
            "sentiment_score": 0.0,
            "run_id": run_id
        })

    # lưu vào transform_temp
    print("Dọn bảng transform_temp cũ...")
    cur_stg.execute("DELETE FROM transformed_temp_table")
    conn_stg.commit()

    print("Ghi dữ liệu mới vào transform_temp...")
    insert_query = """
        INSERT INTO transformed_temp_table (
            article_url, source_name, category_name, author_name,
            title, description, word_count, sentiment_score,
            published_at, run_id
        )
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    """
    for t in transformed:
        cur_stg.execute(insert_query, (
            t["article_url"], t["source_name"], t["category_name"], t["author_name"],
            t["title"], t["description"], t["word_count"], t["sentiment_score"],
            t["published_at"], t["run_id"]
        ))
    conn_stg.commit()
    print(f"Đã lưu {len(transformed)} bản ghi vào transformed_temp_table")

    # load từ transform_temp sang DW
    cur_stg.execute("SELECT * FROM transformed_temp_table")
    data = cur_stg.fetchall()

    inserted_count = 0
    for row in data:
        # DIM Source
        cur_dw.execute("INSERT IGNORE INTO DimSource (sourcename) VALUES (%s)", (row["source_name"],))
        conn_dw.commit()
        cur_dw.execute("SELECT sourcekey FROM DimSource WHERE sourcename=%s", (row["source_name"],))
        sourcekey = cur_dw.fetchone()[0]

        # DIM Category
        cur_dw.execute("INSERT IGNORE INTO DimCategory (categoryname) VALUES (%s)", (row["category_name"],))
        conn_dw.commit()
        cur_dw.execute("SELECT categorykey FROM DimCategory WHERE categoryname=%s", (row["category_name"],))
        categorykey = cur_dw.fetchone()[0]

        # DIM Article
        cur_dw.execute("""
            INSERT IGNORE INTO DimArticle (articleurl, title, description, fullcontent)
            VALUES (%s,%s,%s,%s)
        """, (row["article_url"], row["title"], row["description"], row["fullcontent"]))
        conn_dw.commit()
        cur_dw.execute("SELECT articlekey FROM DimArticle WHERE articleurl=%s", (row["article_url"],))
        articlekey = cur_dw.fetchone()[0]

        # DIM Date
        pub_date = row["published_at"].date() if isinstance(row["published_at"], datetime.datetime) else datetime.date.today()
        cur_dw.execute("""
            INSERT IGNORE INTO DimDate (datekey, fulldate, dayofweekname, dayofmonth, month, monthname, year, quarter, isweekend)
            VALUES (
                %s,%s,DAYNAME(%s),DAY(%s),MONTH(%s),MONTHNAME(%s),YEAR(%s),QUARTER(%s),
                CASE WHEN DAYOFWEEK(%s) IN (1,7) THEN TRUE ELSE FALSE END
            )
        """, (pub_date, pub_date, pub_date, pub_date, pub_date, pub_date, pub_date, pub_date, pub_date))
        conn_dw.commit()

        # FACT Article Metrics
        cur_dw.execute("""
            INSERT INTO FactArticleMetrics (datekey, sourcekey, categorykey, articlekey, wordcount, sentimentscore)
            VALUES (%s,%s,%s,%s,%s,%s)
        """, (pub_date, sourcekey, categorykey, articlekey, row["word_count"], row["sentiment_score"]))
        conn_dw.commit()

        inserted_count += 1

    print(f"Load hoàn tất: {inserted_count} bản ghi được đưa vào Data Warehouse!")

    cur_stg.close()
    cur_dw.close()
    conn_stg.close()
    conn_dw.close()

    print("ETL job hoàn thành thành công!")

if __name__ == "__main__":
    main()
