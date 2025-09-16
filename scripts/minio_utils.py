from io import BytesIO
from minio import Minio
from .config import MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE, MINIO_BUCKET

def get_minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=MINIO_SECURE)

def ensure_bucket(client: Minio, bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

def put_parquet(client: Minio, bucket: str, key: str, table_bytes: bytes):
    client.put_object(bucket, key, data=BytesIO(table_bytes), length=len(table_bytes), content_type="application/octet-stream")
