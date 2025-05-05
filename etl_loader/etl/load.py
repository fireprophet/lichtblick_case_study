from sqlalchemy import create_engine, text
import pandas as pd
import os

def load_to_staging(df: pd.DataFrame, entity: str, file: str, load_id: str):
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "etldb")
    user = os.getenv("POSTGRES_USER", "etluser")
    password = os.getenv("POSTGRES_PASSWORD", "etlpass")

    url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
    engine = create_engine(url)

    filename = os.path.splitext(os.path.basename(file))[0]
    table_name = f"{filename}_staging_{load_id}"

    print(f"[DEBUG] Подключение к PostgreSQL: {url}")
    print(f"[DEBUG] Загружаем {len(df)} строк в таблицу {table_name} из файла {file}")

    with engine.begin() as conn:
        df.to_sql(table_name, con=conn, if_exists="replace", index=False)

        conn.execute(text("""
            CREATE TABLE IF NOT EXISTS load_log (
                load_id TEXT,
                entity TEXT,
                file TEXT,
                status TEXT,
                rows_loaded INTEGER,
                error TEXT
            )
        """))
        conn.execute(text("""
            INSERT INTO load_log (load_id, entity, file, status, rows_loaded, error)
            VALUES (:load_id, :entity, :file, :status, :rows_loaded, :error)
        """), {
            "load_id": load_id,
            "entity": entity,
            "file": file,
            "status": "SUCCESS",
            "rows_loaded": len(df),
            "error": None
        })
