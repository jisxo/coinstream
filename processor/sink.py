import boto3
import clickhouse_connect


def build_clickhouse_client(host: str, port: int, username: str, password: str, database: str):
    return clickhouse_connect.get_client(
        host=host,
        port=port,
        username=username,
        password=password,
        database=database,
    )


def build_s3_client(endpoint_url: str, access_key: str, secret_key: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="us-east-1",
    )


def ensure_s3_bucket(s3_client, bucket_name: str) -> None:
    # Bootstrap target bucket once at processor startup.
    bucket_names = {item["Name"] for item in s3_client.list_buckets().get("Buckets", [])}
    if bucket_name in bucket_names:
        return
    s3_client.create_bucket(Bucket=bucket_name)
