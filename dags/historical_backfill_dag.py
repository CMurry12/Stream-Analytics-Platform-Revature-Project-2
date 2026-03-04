import os
from datetime import datetime

import boto3
import psycopg2
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


DEFAULT_SYMBOLS = "AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA,META"


def load_active_symbols() -> str:
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT symbol
                FROM public.stock_symbols
                WHERE is_active = TRUE
                ORDER BY symbol
                """
            )
            rows = cur.fetchall()
    except psycopg2.Error:
        rows = []
    finally:
        conn.close()

    if not rows:
        return os.getenv("STOCK_SYMBOLS", DEFAULT_SYMBOLS)
    return ",".join(r[0] for r in rows)


def validate_silver_data():
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
    )
    response = client.list_objects_v2(
        Bucket="datalake",
        Prefix="silver/stock_bars_clean/year=",
        MaxKeys=1,
    )
    if response.get("KeyCount", 0) == 0:
        raise ValueError("No silver stock bars found in MinIO")


def validate_gold_data():
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM public.stock_bars_gold")
            row_count = cur.fetchone()[0]
            if row_count == 0:
                raise ValueError("Gold table public.stock_bars_gold is empty")
    finally:
        conn.close()


with DAG(
    dag_id="historical_backfill_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    symbols = PythonOperator(
        task_id="load_active_symbols",
        python_callable=load_active_symbols,
    )

    yfinance_ingest = BashOperator(
        task_id="yfinance_ingest_backfill",
        bash_command=(
            "python /opt/spark-jobs/ingest_yfinance_to_kafka.py "
            "--bootstrap-servers kafka:9092 "
            "--topic stock_bars_raw "
            "--symbols '{{ ti.xcom_pull(task_ids=\"load_active_symbols\") }}' "
            "--period 1y "
            "--interval 1h"
        ),
    )

    spark_to_bronze = BashOperator(
        task_id="spark_to_bronze",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--packages "
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "/opt/spark-jobs/spark_kafka_to_bronze.py "
            "--bootstrap-servers kafka:9092 "
            "--topic stock_bars_raw "
            "--output-path s3a://datalake/bronze/stock_bars"
        ),
    )

    spark_to_silver = BashOperator(
        task_id="spark_to_silver",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--packages "
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "/opt/spark-jobs/spark_bronze_to_silver.py "
            "--input-path s3a://datalake/bronze/stock_bars "
            "--output-path s3a://datalake/silver/stock_bars_clean"
        ),
    )

    validate_silver = PythonOperator(
        task_id="validate_silver",
        python_callable=validate_silver_data,
    )

    spark_to_gold = BashOperator(
        task_id="spark_to_gold",
        bash_command=(
            "spark-submit "
            "--master local[*] "
            "--packages "
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.7.3 "
            "--conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 "
            "--conf spark.hadoop.fs.s3a.access.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.secret.key=minioadmin "
            "--conf spark.hadoop.fs.s3a.path.style.access=true "
            "--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false "
            "--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem "
            "/opt/spark-jobs/spark_silver_to_gold.py "
            "--input-path s3a://datalake/silver/stock_bars_clean "
            "--jdbc-url jdbc:postgresql://postgres:5432/airflow "
            "--user airflow "
            "--password airflow "
            "--table public.stock_bars_gold"
        ),
    )

    validate_gold = PythonOperator(
        task_id="validate_gold",
        python_callable=validate_gold_data,
    )

    symbols >> yfinance_ingest >> spark_to_bronze >> spark_to_silver >> validate_silver >> spark_to_gold >> validate_gold
