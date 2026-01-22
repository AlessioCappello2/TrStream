import os
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url=os.environ["MINIO_ENDPOINT"],
    aws_access_key_id=os.environ["MINIO_ACCESS_KEY"],
    aws_secret_access_key=os.environ["MINIO_SECRET_KEY"]
)

buckets = [
    os.environ["MINIO_INGESTION_BUCKET"], 
    os.environ["MINIO_PROCESSED_BUCKET"], 
    os.environ["MINIO_ANALYTICS_BUCKET"]
]

for bucket in buckets:
    try:
        s3.create_bucket(Bucket=bucket)
        print(f"Bucket '{bucket}' created successfully!")
    except Exception:
        print(f"Bucket '{bucket}' already exists, skipping...")
