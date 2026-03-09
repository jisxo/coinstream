import argparse
import os
from io import BytesIO
from typing import Iterator

import boto3
import orjson
import pyarrow.parquet as pq
from confluent_kafka import Producer


def list_objects(s3, bucket: str, prefix: str, limit: int | None = None) -> Iterator[str]:
    paginator = s3.get_paginator("list_objects_v2")
    count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if limit is not None and count >= limit:
                return
            key = obj["Key"]
            if key.endswith(".parquet"):
                yield key
                count += 1


def read_parquet_from_s3(s3, bucket: str, key: str) -> list[dict]:
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read()
    table = pq.read_table(BytesIO(data))
    return table.to_pylist()


def create_producer(brokers: str) -> Producer:
    return Producer({"bootstrap.servers": brokers})


def produce_rows(producer: Producer, topic: str, rows: list[dict]) -> None:
    for row in rows:
        producer.produce(topic, key=None, value=orjson.dumps(row))
        producer.poll(0)
    producer.flush()


def main() -> None:
    parser = argparse.ArgumentParser(description="Replay Parquet files from S3 to Kafka")
    parser.add_argument("--prefix", help="S3 prefix (default: env S3_PREFIX)", default=os.getenv("S3_PREFIX", "ohlc_1m"))
    parser.add_argument("--limit", type=int, help="Max number of files to replay")
    args = parser.parse_args()

    s3 = boto3.client(
        "s3",
        endpoint_url=os.getenv("S3_ENDPOINT", "http://minio:9000"),
        aws_access_key_id=os.getenv("S3_ACCESS_KEY", "minioadmin"),
        aws_secret_access_key=os.getenv("S3_SECRET_KEY", "minioadmin"),
        region_name=os.getenv("S3_REGION", "us-east-1"),
    )
    producer = create_producer(os.getenv("KAFKA_BROKERS", "redpanda:9092"))
    topic = os.getenv("REPLAY_TOPIC", os.getenv("TOPIC_TRADES", "trades_raw"))
    bucket = os.getenv("S3_BUCKET", "crypto")

    for key in list_objects(s3, bucket, args.prefix, limit=args.limit):
        rows = read_parquet_from_s3(s3, bucket, key)
        produce_rows(producer, topic, rows)
        print(f"replayed {len(rows)} records from {key}")


if __name__ == "__main__":
    main()
