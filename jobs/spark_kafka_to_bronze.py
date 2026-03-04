"""
Read stock events from Kafka and write bronze parquet to MinIO.
"""
import argparse

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from spark_session_factory import create_spark_session


def run_job(
    master: str,
    bootstrap_servers: str,
    topic: str,
    output_path: str,
):
    spark = create_spark_session(app_name="stock-kafka-to-bronze", master=master)
    try:
        source_df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", bootstrap_servers)
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .option("endingOffsets", "latest")
            .load()
        )
        if source_df.rdd.isEmpty():
            print("No Kafka messages found for bronze load.")
            return

        schema = StructType(
            [
                StructField("symbol", StringType(), True),
                StructField("event_time", StringType(), True),
                StructField("open", DoubleType(), True),
                StructField("high", DoubleType(), True),
                StructField("low", DoubleType(), True),
                StructField("close", DoubleType(), True),
                StructField("adj_close", DoubleType(), True),
                StructField("volume", LongType(), True),
                StructField("source", StringType(), True),
                StructField("emitted_at_utc", StringType(), True),
            ]
        )

        parsed_df = (
            source_df.selectExpr("CAST(value AS STRING) AS payload")
            .select(F.from_json("payload", schema).alias("j"))
            .select("j.*")
            .dropna(subset=["symbol", "event_time"])
            .withColumn("event_ts", F.to_timestamp("event_time"))
            .dropna(subset=["event_ts"])
            .withColumn("year", F.date_format("event_ts", "yyyy"))
            .withColumn("month", F.date_format("event_ts", "MM"))
            .withColumn("day", F.date_format("event_ts", "dd"))
        )

        valid_condition = (
            F.col("open").isNotNull()
            & F.col("high").isNotNull()
            & F.col("low").isNotNull()
            & F.col("close").isNotNull()
            & F.col("volume").isNotNull()
            & (F.col("open") > 0)
            & (F.col("high") > 0)
            & (F.col("low") > 0)
            & (F.col("close") > 0)
            & (F.col("volume") >= 0)
            & (F.col("high") >= F.col("low"))
            & (F.col("high") >= F.col("open"))
            & (F.col("high") >= F.col("close"))
            & (F.col("low") <= F.col("open"))
            & (F.col("low") <= F.col("close"))
        )

        invalid_count = parsed_df.filter(~valid_condition).count()
        valid_df = parsed_df.filter(valid_condition).dropDuplicates(["symbol", "event_ts"])
        valid_count = valid_df.count()
        print(f"Bronze quality check: valid_rows={valid_count}, invalid_rows={invalid_count}")
        if valid_count == 0:
            raise ValueError("Bronze write aborted: no valid stock bars after quality checks.")

        (
            valid_df.write.mode("overwrite")
            .partitionBy("year", "month", "day")
            .parquet(output_path.rstrip("/") + "/")
        )
    finally:
        spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka to MinIO bronze")
    parser.add_argument("--master", default="local[*]")
    parser.add_argument("--bootstrap-servers", default="kafka:9092")
    parser.add_argument("--topic", default="stock_bars_raw")
    parser.add_argument(
        "--output-path",
        default="s3a://datalake/bronze/stock_bars",
    )
    args = parser.parse_args()
    run_job(
        master=args.master,
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        output_path=args.output_path,
    )
