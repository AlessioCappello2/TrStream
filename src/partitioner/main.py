####################################################################
# IMPORTS #
####################################################################
import boto3
import pyarrow as pa
from datetime import datetime

from partitioner.config.settings import settings
from partitioner.config.load_config import load_config
from partitioner.config.logging_config import setup_logging

from partitioner.core.reader import S3Reader 
from partitioner.core.writer import S3Writer
from partitioner.core.deleter import S3Deleter
from partitioner.core.partitioning import normalize_timestamp, split_partitions

####################################################################
# Logging
####################################################################
logger = setup_logging()

####################################################################
# Env variables
####################################################################
bucket_src = settings.bucket_src
bucket_trg = settings.bucket_trg

minio_endpoint = settings.minio_endpoint
access_key = settings.minio_access_key
secret_key = settings.minio_secret_key

# Main function
def main():
        
    logger.info(f"Partitioner job started at: {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}")
    
    ####################################################################
    # Config reading
    #################################################################### 
    cfg = load_config()
    batch_size = cfg['batch_size']

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
        bucket=bucket_src
    )

    writer = S3Writer(
        s3_client=s3,
        bucket=bucket_trg
    )

    deleter = S3Deleter(
        s3_client=s3,
        bucket=bucket_src
    )
    
    ####################################################################
    # Partitioning logic
    ####################################################################

    # Overcome the limitation of list_objects_v2 that retrieves up to 1000 objects by enabling a page approach
    continuation = None

    while True:
        # Pagination exploiting boto3 API (continuation token and truncated)
        keys, continuation, truncated = reader.list_objects_page(continuation)
        parquet_keys = [k for k in keys if k.endswith('.parquet')]
        if not parquet_keys:
            if not truncated:
                break
            continue
        
        # Batching witin the page
        for i in range(0, len(parquet_keys), batch_size):
            batch = parquet_keys[i:i+batch_size]

            # Read Parquet files of batch keys
            tables = reader.read_parquets(batch)
            if not tables:
                deleter.delete_objects(batch)
                continue 
            
            # Group records by partition keys and write
            table = pa.concat_tables(tables)
            table = normalize_timestamp(table)

            partitions = split_partitions(table)

            for year, month, day, ttype, part in partitions:
                writer.write_partition(year, month, day, ttype, part)

            # Batch cleanup
            deleter.delete_objects(batch)

        if not truncated:
            break

    logger.info(f"Partitioner job ended successfully at {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}. Exiting the container now...")


if __name__ == '__main__':
    main()
