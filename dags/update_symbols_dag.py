import os
from datetime import datetime

import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator


DEFAULT_SYMBOLS = "AAPL,MSFT,GOOGL,AMZN,TSLA,NVDA,META"


def update_symbols_table():
    symbols_csv = os.getenv("STOCK_SYMBOLS", DEFAULT_SYMBOLS)
    symbols = sorted({s.strip().upper() for s in symbols_csv.split(",") if s.strip()})
    if not symbols:
        raise ValueError("No symbols provided for update_symbols_dag")

    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "airflow"),
        user=os.getenv("POSTGRES_USER", "airflow"),
        password=os.getenv("POSTGRES_PASSWORD", "airflow"),
    )
    try:
        with conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    CREATE TABLE IF NOT EXISTS public.stock_symbols (
                        symbol TEXT PRIMARY KEY,
                        is_active BOOLEAN NOT NULL DEFAULT TRUE,
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                cur.execute("UPDATE public.stock_symbols SET is_active = FALSE, updated_at = NOW()")
                for symbol in symbols:
                    cur.execute(
                        """
                        INSERT INTO public.stock_symbols (symbol, is_active, updated_at)
                        VALUES (%s, TRUE, NOW())
                        ON CONFLICT (symbol)
                        DO UPDATE SET is_active = EXCLUDED.is_active, updated_at = EXCLUDED.updated_at
                        """,
                        (symbol,),
                    )
        print(f"Updated stock_symbols table with {len(symbols)} active symbols")
    finally:
        conn.close()


with DAG(
    dag_id="update_symbols_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 * * *",
    catchup=False,
) as dag:
    update_symbols = PythonOperator(
        task_id="update_symbols",
        python_callable=update_symbols_table,
    )

