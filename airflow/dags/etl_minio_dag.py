from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import os
import sqlite3
import boto3
import requests
import psycopg2
from airflow.exceptions import AirflowFailException
from psycopg2 import sql
import re
from collections import defaultdict

# Default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False
}

def create_staging_views(**context):
    suffix = context['ti'].xcom_pull(key='load_id', task_ids='generate_load_id')
    if not suffix:
        raise AirflowFailException("Suffix (load_id) не найден в XCom")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        dbname=os.getenv("POSTGRES_DB", "etldb"),
        user=os.getenv("POSTGRES_USER", "etluser"),
        password=os.getenv("POSTGRES_PASSWORD", "etlpass"),
        port=os.getenv("POSTGRES_PORT", 5432)
    )
    conn.autocommit = True

    columns_by_table = {
        "products": [
            ("id", "bigint"),
            ("deleted", "smallint"),
            ("releasedversion", "text"),
            ("productcode", "text"),
            ("productname", "text"),
            ("energy", "text"),
            ("consumptiontype", "text"),
            ("modificationdate", "timestamp")
        ],
        "prices": [
            ("id", "bigint"),
            ("productid", "int"),
            ("pricecomponentid", "int"),
            ("pricecomponent", "text"),
            ("price", "numeric(38,10)"),
            ("unit", "text"),
            ("valid_from", "timestamp"),
            ("valid_until", "timestamp"),
            ("modificationdate", "timestamp")
        ],
        "contracts": [
            ("id", "bigint"),
            ("type", "text"),
            ("energy", "text"),
            ("usage", "int"),
            ("usagenet", "int"),
            ("createdat", "timestamp"),
            ("startdate", "timestamp"),
            ("enddate", "timestamp"),
            ("filingdatecancellation", "timestamp"),
            ("cancellationreason", "text"),
            ("city", "text"),
            ("status", "text"),
            ("productid", "int"),
            ("modificationdate", "timestamp")
        ]
    }

    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT tablename
                FROM pg_tables
                WHERE schemaname = 'public'
                  AND tablename LIKE %s
            """, (f"%_staging_{suffix}",))

            all_tables = [row[0] for row in cur.fetchall()]
            if not all_tables:
                return

            cur.execute("CREATE SCHEMA IF NOT EXISTS staging")

            grouped = defaultdict(list)
            for table in all_tables:
                match = re.search(r"(products|prices|contracts)_staging", table)
                if match:
                    base = match.group(1)
                    grouped[base].append(table)
                else:
                    raise AirflowFailException(f"Invalid table name: {table}")

            for base, tables in grouped.items():
                view_name = f"staging.{base}_staging_view"
                columns = columns_by_table[base]

                union_sql_parts = []
                for table in tables:
                    col_casts = ", ".join(
                        f"{col}::{dtype} AS {col}" for col, dtype in columns
                    ) + f", '{table}'::text AS source_table"
                    union_sql_parts.append(f"SELECT {col_casts} FROM public.\"{table}\"")

                full_union_sql = "\nUNION ALL\n".join(union_sql_parts)

                final_sql = f"""
                    CREATE OR REPLACE VIEW {view_name} AS
                    {full_union_sql};
                """
                cur.execute(final_sql)

    finally:
        conn.close()

def generate_load_id(**context):
    now = datetime.utcnow()
    load_id = now.strftime("%Y%m%d_%H%M%S%f")[:-3]
    context['ti'].xcom_push(key='load_id', value=load_id)

def call_etl_api(entity: str, **context):
    params = context["params"]
    end_month = params.get("END_MONTH")
    start_month = params.get("START_MONTH") or end_month
    load_id = context['ti'].xcom_pull(key='load_id', task_ids='generate_load_id')

    response = requests.post(
        "http://etl-runner:8888/run",
        json={"entity": entity, "start_month": start_month, "end_month": end_month, "load_id": load_id}
    )
    response.raise_for_status()
    return response.json()

def log_dag_event(context, status):
    try:
        db_path = os.getenv("SQLITE_DB_PATH", "/opt/airflow/sqlite/etl.db")
        load_id = context['ti'].xcom_pull(key='load_id', task_ids='generate_load_id') or context['run_id']
        task_id = context['task_instance'].task_id
        error = str(context.get('exception')) if status == 'FAILED' else None
        with sqlite3.connect(db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS load_log (
                    load_id TEXT,
                    entity TEXT,
                    file TEXT,
                    status TEXT,
                    rows_loaded INTEGER,
                    error TEXT
                )""")
            conn.execute("""
                INSERT INTO load_log (load_id, entity, file, status, rows_loaded, error)
                VALUES (?, ?, ?, ?, ?, ?)""",
                (load_id, task_id, "N/A", status, 0, error))
            conn.commit()
    except Exception as e:
        print(f"[LOGGING ERROR] Could not write log for {task_id}: {e}")

def check_minio_and_db():
    db_path = os.getenv("SQLITE_DB_PATH", "/opt/airflow/sqlite/etl.db")
    with sqlite3.connect(db_path, timeout=5) as conn:
        conn.execute("SELECT 1;")
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv("MINIO_ENDPOINT"),
        aws_access_key_id=os.getenv("MINIO_ACCESS_KEY"),
        aws_secret_access_key=os.getenv("MINIO_SECRET_KEY")
    )
    s3.head_bucket(Bucket=os.getenv("MINIO_BUCKET", "srcdata"))

def run_sql_file(path):
    with open(path, "r") as f:
        sql_code = f.read()
    conn = psycopg2.connect(
        host="postgres",
        dbname="etldb",
        user="etluser",
        password="etlpass"
    )
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(sql_code)
    conn.close()

def create_dwh_tables():
    run_sql_file("/opt/airflow/sql/create_dwh.sql")

def load_products():
    run_sql_file("/opt/airflow/sql/load_products.sql")

def load_prices():
    run_sql_file("/opt/airflow/sql/load_prices.sql")

def load_contracts():
    run_sql_file("/opt/airflow/sql/load_contracts.sql")

def load_price_periods():
    run_sql_file("/opt/airflow/sql/load_price_periods.sql")

def load_contracts_enriched():
    run_sql_file("/opt/airflow/sql/load_contracts_enriched.sql")

with DAG(
    dag_id='etl_minio_dag',
    default_args=default_args,
    schedule_interval="0 22 1 * *",  
    catchup=False,
    start_date=datetime(2020, 1, 1),
    params={
        "ENTITY": Param("products,prices,contracts", type="string"),
        "START_MONTH": Param("202101", type="string"),
        "END_MONTH": Param("202103", type="string")
    }
) as dag:

    t_check_minio_and_db = PythonOperator(
        task_id='check_minio_and_db',
        python_callable=check_minio_and_db,
        on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
        on_failure_callback=lambda context: log_dag_event(context, "FAILED")
    )

    t_generate_load_id = PythonOperator(
        task_id='generate_load_id',
        python_callable=generate_load_id
    )

    t_create_dwh_tables = PythonOperator(
        task_id='create_dwh_tables',
        python_callable=create_dwh_tables,
        on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
        on_failure_callback=lambda context: log_dag_event(context, "FAILED")
    )

    with TaskGroup("load_entities") as load_group:
        t_products = PythonOperator(
            task_id='call_etl_api_products',
            python_callable=call_etl_api,
            op_args=["products"],
            on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
            on_failure_callback=lambda context: log_dag_event(context, "FAILED")
        )
        t_prices = PythonOperator(
            task_id='call_etl_api_prices',
            python_callable=call_etl_api,
            op_args=["prices"],
            on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
            on_failure_callback=lambda context: log_dag_event(context, "FAILED")
        )
        t_contracts = PythonOperator(
            task_id='call_etl_api_contracts',
            python_callable=call_etl_api,
            op_args=["contracts"],
            on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
            on_failure_callback=lambda context: log_dag_event(context, "FAILED")
        )
        t_products >> t_prices >> t_contracts

    t_create_staging_views = PythonOperator(
        task_id='create_staging_views',
        python_callable=create_staging_views,
        on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
        on_failure_callback=lambda context: log_dag_event(context, "FAILED")
    )

    t_load_products = PythonOperator(
        task_id='load_products',
        python_callable=load_products,
        on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
        on_failure_callback=lambda context: log_dag_event(context, "FAILED")
    )

    t_load_prices = PythonOperator(
        task_id='load_prices',
        python_callable=load_prices,
        on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
        on_failure_callback=lambda context: log_dag_event(context, "FAILED")
    )

    t_load_contracts = PythonOperator(
        task_id='load_contracts',
        python_callable=load_contracts,
        on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
        on_failure_callback=lambda context: log_dag_event(context, "FAILED")
    )

    t_load_price_periods = PythonOperator(
        task_id='load_price_periods',
        python_callable=load_price_periods,
        on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
        on_failure_callback=lambda context: log_dag_event(context, "FAILED")
    )

    t_load_contracts_enriched = PythonOperator(
        task_id='load_contracts_enriched',
        python_callable=load_contracts_enriched,
        on_success_callback=lambda context: log_dag_event(context, "SUCCESS"),
        on_failure_callback=lambda context: log_dag_event(context, "FAILED")
    )

    (
        t_check_minio_and_db 
        >> t_generate_load_id 
        >> t_create_dwh_tables 
        >> load_group 
        >> t_create_staging_views 
        >> [t_load_products, t_load_prices, t_load_contracts] 
        >> t_load_price_periods 
        >> t_load_contracts_enriched
    )