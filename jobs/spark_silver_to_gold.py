"""
Feature engineering on silver stock bars and load to Postgres gold table.
"""
import argparse
from urllib.parse import urlparse

import psycopg2
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_session_factory import create_spark_session


def parse_jdbc_url(jdbc_url: str):
    if not jdbc_url.startswith("jdbc:postgresql://"):
        raise ValueError(f"Unsupported JDBC URL: {jdbc_url}")
    parsed = urlparse(jdbc_url.replace("jdbc:", "", 1))
    return parsed.hostname, parsed.port or 5432, parsed.path.lstrip("/")


def split_table_name(table: str):
    if "." not in table:
        return "public", table
    schema, table_name = table.split(".", 1)
    return schema, table_name


def merge_to_gold(jdbc_url: str, user: str, password: str, target_table: str, staging_table: str):
    host, port, dbname = parse_jdbc_url(jdbc_url)
    tgt_schema, tgt_table = split_table_name(target_table)
    stg_schema, stg_table = split_table_name(staging_table)

    conn = psycopg2.connect(host=host, port=port, dbname=dbname, user=user, password=password)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {tgt_schema}.{tgt_table} (
                    symbol TEXT NOT NULL,
                    event_ts TIMESTAMP NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    adj_close DOUBLE PRECISION,
                    volume BIGINT,
                    return_pct DOUBLE PRECISION,
                    sma_5 DOUBLE PRECISION
                )
                """
            )
            cur.execute(
                f"""
                CREATE UNIQUE INDEX IF NOT EXISTS ux_{tgt_table}_symbol_event_ts
                ON {tgt_schema}.{tgt_table} (symbol, event_ts)
                """
            )
            cur.execute(
                f"""
                MERGE INTO {tgt_schema}.{tgt_table} AS t
                USING {stg_schema}.{stg_table} AS s
                  ON t.symbol = s.symbol
                 AND t.event_ts = s.event_ts
                WHEN MATCHED THEN UPDATE SET
                    open = s.open,
                    high = s.high,
                    low = s.low,
                    close = s.close,
                    adj_close = s.adj_close,
                    volume = s.volume,
                    return_pct = s.return_pct,
                    sma_5 = s.sma_5
                WHEN NOT MATCHED THEN INSERT
                    (symbol, event_ts, open, high, low, close, adj_close, volume, return_pct, sma_5)
                VALUES
                    (s.symbol, s.event_ts, s.open, s.high, s.low, s.close, s.adj_close, s.volume, s.return_pct, s.sma_5)
                """
            )
            cur.execute(f"DROP TABLE IF EXISTS {stg_schema}.{stg_table}")
    finally:
        conn.close()


def run_job(
    master: str,
    input_path: str,
    jdbc_url: str,
    user: str,
    password: str,
    table: str,
):
    spark = create_spark_session(app_name="stock-silver-to-gold", master=master)
    try:
        df = spark.read.parquet(input_path.rstrip("/") + "/")
        if df.rdd.isEmpty():
            raise ValueError("Gold load aborted: silver dataset is empty.")

        w = Window.partitionBy("symbol").orderBy("event_ts")
        features = (
            df.withColumn("prev_close", F.lag("close").over(w))
            .withColumn(
                "return_pct",
                F.when(F.col("prev_close").isNull(), None).otherwise(
                    ((F.col("close") - F.col("prev_close")) / F.col("prev_close")) * 100.0
                ),
            )
            .withColumn("sma_5", F.avg("close").over(w.rowsBetween(-4, 0)))
            .select(
                "symbol",
                "event_ts",
                "open",
                "high",
                "low",
                "close",
                "adj_close",
                "volume",
                "return_pct",
                "sma_5",
            )
            .dropna(subset=["symbol", "event_ts", "open", "high", "low", "close"])
            .filter(F.col("open") > 0)
            .filter(F.col("high") > 0)
            .filter(F.col("low") > 0)
            .filter(F.col("close") > 0)
            .filter(F.col("volume").isNotNull())
            .filter(F.col("volume") >= 0)
            .filter(F.col("high") >= F.col("low"))
            .filter(F.col("high") >= F.col("open"))
            .filter(F.col("high") >= F.col("close"))
            .filter(F.col("low") <= F.col("open"))
            .filter(F.col("low") <= F.col("close"))
            .dropDuplicates(["symbol", "event_ts"])
        )

        out_count = features.count()
        print(f"Gold quality check: output_rows={out_count}")
        if out_count == 0:
            raise ValueError("Gold load aborted: no valid rows after quality checks.")

        staging_table = table.replace(".", ".__staging_", 1) if "." in table else f"{table}__staging"
        (
            features.write.mode("overwrite")
            .format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", staging_table)
            .option("user", user)
            .option("password", password)
            .option("driver", "org.postgresql.Driver")
            .save()
        )

        merge_to_gold(
            jdbc_url=jdbc_url,
            user=user,
            password=password,
            target_table=table,
            staging_table=staging_table,
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver to Gold feature load")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--input-path", default="s3a://datalake/silver/stock_bars_clean")
    parser.add_argument(
        "--jdbc-url",
        default="jdbc:postgresql://postgres:5432/airflow",
    )
    parser.add_argument("--user", default="airflow")
    parser.add_argument("--password", default="airflow")
    parser.add_argument("--table", default="public.stock_bars_gold")
    args = parser.parse_args()

    run_job(
        master=args.master,
        input_path=args.input_path,
        jdbc_url=args.jdbc_url,
        user=args.user,
        password=args.password,
        table=args.table,
    )
