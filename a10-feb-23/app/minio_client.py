import io
import boto3
from botocore.client import Config
from app.config import (
    MINIO_ENDPOINT,
    MINIO_ACCESS_KEY,
    MINIO_SECRET_KEY,
    MINIO_SECURE,
)


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=f"http://{MINIO_ENDPOINT}",
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def ensure_bucket_exists(client, bucket: str):
    try:
        client.head_bucket(Bucket=bucket)
    except client.exceptions.ClientError:
        client.create_bucket(Bucket=bucket)


def upload_object(client, bucket: str, object_key: str, data: bytes, content_type: str = "image/jpeg"):
    client.put_object(
        Bucket=bucket,
        Key=object_key,
        Body=data,
        ContentType=content_type,
    )
    return f"http://{MINIO_ENDPOINT}/{bucket}/{object_key}"


def download_object(client, bucket: str, object_key: str) -> bytes:
    response = client.get_object(Bucket=bucket, Key=object_key)
    return response["Body"].read()


def delete_object(client, bucket: str, object_key: str):
    client.delete_object(Bucket=bucket, Key=object_key)


def object_exists(client, bucket: str, object_key: str) -> bool:
    try:
        client.head_object(Bucket=bucket, Key=object_key)
        return True
    except client.exceptions.ClientError:
        return False
