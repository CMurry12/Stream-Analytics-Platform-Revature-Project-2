"""
Kafka Batch Consumer - Ingest to Landing Zone

Consumes messages from Kafka for a time window and writes to landing zone as JSON.

Pattern: Kafka Topic -> (This Script) -> ./data/landing/{topic}_{timestamp}.json
"""
from kafka import KafkaConsumer
import json
import time
import os
import argparse
from datetime import datetime, timezone

import boto3


def consume_batch(topic: str, batch_duration_sec: int, output_path: str) -> int:
    """
    Consume from Kafka for specified duration and write to landing zone.
    
    Args:
        topic: Kafka topic to consume from
        batch_duration_sec: How long to consume before writing
        output_path: Directory to write output JSON files
        
    Returns:
        Number of messages consumed
    """
    bootstrap_servers = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    consumer_group = os.getenv("KAFKA_CONSUMER_GROUP", "streamflow-ingestor")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="latest",
        enable_auto_commit=False,
        consumer_timeout_ms=1000,
        group_id=consumer_group,
    )

    deadline = time.time() + batch_duration_sec
    records = []

    try:
        while time.time() < deadline:
            batch = consumer.poll(timeout_ms=500, max_records=500)
            for partition_records in batch.values():
                for msg in partition_records:
                    try:
                        payload = json.loads(msg.value.decode("utf-8"))
                    except json.JSONDecodeError:
                        payload = {"raw": msg.value.decode("utf-8", errors="replace")}

                    payload["_kafka_topic"] = msg.topic
                    payload["_kafka_partition"] = msg.partition
                    payload["_kafka_offset"] = msg.offset
                    payload["_ingested_at_utc"] = datetime.now(timezone.utc).isoformat()
                    records.append(payload)
    finally:
        consumer.close()

    if not records:
        return 0

    output_path = output_path.rstrip("/")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    file_name = f"{topic}_{ts}.json"

    if output_path.startswith("s3a://"):
        s3_uri = output_path.replace("s3a://", "", 1)
        bucket, key_prefix = s3_uri.split("/", 1) if "/" in s3_uri else (s3_uri, "")
        object_key = f"{key_prefix}/{file_name}" if key_prefix else file_name

        client = boto3.client(
            "s3",
            endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
            aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
            region_name=os.getenv("S3_REGION", "us-east-1"),
        )
        body = "\n".join(json.dumps(r) for r in records).encode("utf-8")
        client.put_object(Bucket=bucket, Key=object_key, Body=body)
    else:
        os.makedirs(output_path, exist_ok=True)
        file_path = os.path.join(output_path, file_name)
        with open(file_path, "w", encoding="utf-8") as f:
            for row in records:
                f.write(json.dumps(row))
                f.write("\n")

    return len(records)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Consume Kafka messages into landing zone")
    parser.add_argument("--topic", required=True)
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--output-path", default="s3a://streamflow-bronze/landing")
    args = parser.parse_args()

    count = consume_batch(args.topic, args.duration, args.output_path)
    print(f"Consumed {count} messages from topic '{args.topic}'")
