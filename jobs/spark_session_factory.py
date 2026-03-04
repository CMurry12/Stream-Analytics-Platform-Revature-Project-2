"""
SparkSession Factory Module

Provides factory functions for creating SparkSession instances.
"""
import os
from typing import Optional

from pyspark.sql import SparkSession


def create_spark_session(
    app_name: str,
    master: str = "local[*]",
    config_overrides: Optional[dict] = None,
) -> SparkSession:
    """
    Create and return a configured SparkSession.
    
    Args:
        app_name: Name for the Spark application
        master: Spark master URL ("local[*]" or "spark://spark-master:7077")
        config_overrides: Optional dict of Spark configurations
        
    Returns:
        Configured SparkSession instance
    """
    builder = SparkSession.builder.appName(app_name).master(master)

    default_conf = {
        "spark.hadoop.fs.s3a.endpoint": os.getenv("S3_ENDPOINT", "http://minio:9000"),
        "spark.hadoop.fs.s3a.access.key": os.getenv("S3_ACCESS_KEY", "minioadmin"),
        "spark.hadoop.fs.s3a.secret.key": os.getenv("S3_SECRET_KEY", "minioadmin"),
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        # Overwrite only touched partitions for idempotent reruns.
        "spark.sql.sources.partitionOverwriteMode": "dynamic",
        "spark.jars.packages": (
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262"
        ),
    }

    if config_overrides:
        default_conf.update(config_overrides)

    for key, value in default_conf.items():
        builder = builder.config(key, value)

    return builder.getOrCreate()
