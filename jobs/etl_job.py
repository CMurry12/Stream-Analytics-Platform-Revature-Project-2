"""
StreamFlow ETL Job - PySpark Transformation Pipeline

Reads JSON from landing zone, applies transformations, writes CSV to gold zone.

Pattern: ./data/landing/*.json -> (This Job) -> ./data/gold/
"""
import argparse

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from spark_session_factory import create_spark_session


def run_etl(spark: SparkSession, input_path: str, output_path: str):
    """
    Main ETL pipeline: read -> transform -> write.
    
    Args:
        spark: Active SparkSession
        input_path: Landing zone path (e.g., '/opt/spark-data/landing/*.json')
        output_path: Gold zone path (e.g., '/opt/spark-data/gold')
    """
    df = spark.read.json(input_path)

    # Minimal bronze -> silver shaping
    if "_ingested_at_utc" in df.columns:
        df = df.withColumn("_ingested_ts", F.to_timestamp("_ingested_at_utc"))
    else:
        df = df.withColumn("_ingested_ts", F.current_timestamp())

    df = (
        df.withColumn("ingest_date", F.to_date("_ingested_ts"))
        .withColumn("ingest_hour", F.hour("_ingested_ts"))
    )

    (
        df.write.mode("append")
        .partitionBy("ingest_date", "ingest_hour")
        .parquet(output_path)
    )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Spark ETL from landing to silver")
    parser.add_argument("--input-path", default="s3a://streamflow-bronze/landing/*.json")
    parser.add_argument("--output-path", default="s3a://streamflow-silver/silver/events")
    parser.add_argument("--master", default="spark://spark-master:7077")
    args = parser.parse_args()

    spark = create_spark_session(app_name="streamflow-etl", master=args.master)
    try:
        run_etl(spark=spark, input_path=args.input_path, output_path=args.output_path)
    finally:
        spark.stop()
