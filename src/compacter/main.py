####################################################################
# IMPORTS #
####################################################################
import io
import os
import math
import boto3
import logging
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from reader import S3Reader
from writer import S3Writer
from deleter import S3Deleter
from settings import settings
from datetime import datetime, timedelta
from logging_config import setup_logging
from config import load_config, resolve_dates
from compaction import group_by_transaction_type, compact_tables

####################################################################
# Logging
####################################################################
setup_logging()
logger = logging.getLogger("trstream.compacter")

####################################################################
# Env variables
####################################################################
bucket_name = settings.bucket_name 

minio_endpoint = settings.minio_endpoint
access_key = settings.minio_access_key
secret_key = settings.minio_secret_key

target_size = settings.target_size


if __name__ == '__main__':

    logger.info(f"Compacter job started at: {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}")
    
    ####################################################################
    # Config reading
    #################################################################### 
    cfg = load_config()
    target_size = cfg.get("target_size_bytes", 256*1024*1024)  # 256 MB default
    dates = resolve_dates(cfg)

    ####################################################################
    # S3 client, S3Reader, S3Writer and S3Deleter instantiation
    ####################################################################
    s3 = boto3.client(
        's3', 
        endpoint_url=minio_endpoint, 
        aws_access_key_id=access_key, 
        aws_secret_access_key=secret_key
    )

    reader = S3Reader(
        s3_client=s3,
        bucket=bucket_name
    )

    writer = S3Writer(
        s3_client=s3,
        bucket=bucket_name
    )

    deleter = S3Deleter(
        s3_client=s3,
        bucket=bucket_name
    )

    ####################################################################
    # Compaction logic
    ####################################################################

    # Files compaction for performance optimization
    for date in dates:
        prefix = f'year={date.year}/month={date.month:02d}/day={date.day:02d}/'

        continuation = None 
        date_keys = []
        while True:
            keys, continuation, truncated = reader.list_objects_page(prefix, continuation)

            if keys:
                date_keys.extend(keys)

            if not truncated:
                break

        groups = group_by_transaction_type(date_keys)

        for ttype, keys in groups.items():
            tables = reader.read_parquets(keys)
            if not tables:
                logger.warning(f"No valid parquet files for {ttype} in {date.strftime('%Y-%m-%d')}. Skipping...")
                continue 

            compacted = compact_tables(tables, target_size)

            try:
                for i, table in enumerate(compacted):
                    writer.write_compacted_partition(date.year, date.month, date.day, ttype, i, table)
            except Exception:
                logger.exception(f"Compaction failed for transaction type {ttype}, skipping delete...")
                continue

            deleter.delete_objects(keys)

    logger.info(f"Compacter job ended successfully at {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}. Exiting the container now...")
