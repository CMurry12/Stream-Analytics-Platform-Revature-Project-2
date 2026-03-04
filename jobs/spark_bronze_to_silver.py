"""
Clean bronze stock bars and write silver parquet to MinIO.
"""
import argparse

from pyspark.sql import functions as F

from spark_session_factory import create_spark_session


def run_job(master: str, input_path: str, output_path: str):
    spark = create_spark_session(app_name="stock-bronze-to-silver", master=master)
    try:
        df = spark.read.parquet(input_path.rstrip("/") + "/")

        clean_df = (
            df.dropna(subset=["symbol", "event_ts", "open", "high", "low", "close"])
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
            .withColumn("volume", F.coalesce(F.col("volume"), F.lit(0)))
            .dropDuplicates(["symbol", "event_ts"])
            .withColumn("year", F.date_format("event_ts", "yyyy"))
            .withColumn("month", F.date_format("event_ts", "MM"))
            .withColumn("day", F.date_format("event_ts", "dd"))
        )

        source_count = df.count()
        clean_count = clean_df.count()
        print(f"Silver quality check: source_rows={source_count}, clean_rows={clean_count}")
        if clean_count == 0:
            raise ValueError("Silver write aborted: no rows passed quality checks.")

        (
            clean_df.write.mode("overwrite")
            .partitionBy("year", "month", "day")
            .parquet(output_path.rstrip("/") + "/")
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Bronze to Silver cleaning")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument(
        "--input-path",
        default="s3a://datalake/bronze/stock_bars",
    )
    parser.add_argument(
        "--output-path",
        default="s3a://datalake/silver/stock_bars_clean",
    )
    args = parser.parse_args()
    run_job(master=args.master, input_path=args.input_path, output_path=args.output_path)
