####################################################################
# IMPORTS #
####################################################################
import boto3
from datetime import datetime

from compacter.config.settings import settings
from shared.config.logging_config import setup_logging
from shared.config.load_config import load_config_from_directory
from pathlib import Path

from compacter.config.dates import resolve_dates

from compacter.core.reader import CompacterS3ParquetReader
from compacter.core.writer import CompacterS3ParquetWriter
from compacter.core.compacting import compact_tables

####################################################################
# Logging
####################################################################
logger = setup_logging("trstream.compacter")

####################################################################
# Env variables
####################################################################
bucket_src = settings.minio_source_bucket
bucket_trg = settings.minio_target_bucket

minio_endpoint = settings.minio_endpoint
access_key = settings.minio_access_key
secret_key = settings.minio_secret_key

# Main function
def main():

    logger.info(f"Compacter job started at: {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}")
    
    ####################################################################
    # Config reading
    #################################################################### 
    cfg = load_config_from_directory(Path("src"), "compacter.yaml")
    target_size = cfg.get("target_size_bytes", 256*1024*1024)  # 256 MB default
    dates = resolve_dates(cfg)
    sources = cfg['compaction']['sources']

    ####################################################################
    # S3 client, S3Reader and S3Writer instantiation
    ####################################################################
    s3 = boto3.client(
        's3', 
        endpoint_url=minio_endpoint, 
        aws_access_key_id=access_key, 
        aws_secret_access_key=secret_key
    )

    reader = CompacterS3ParquetReader(
        s3_client=s3,
        bucket=bucket_src
    )

    writer = CompacterS3ParquetWriter(
        s3_client=s3,
        bucket=bucket_trg
    )

    ####################################################################
    # Compaction logic
    ####################################################################

    for date in dates:
        for source in sources:
            # Compact day by day per source
            prefix = (
                f"source={source}/"
                f"year={date.year}/month={date.month:02d}/day={date.day:02d}/"
            )

            logger.info(f"Compacting data for {source} on {date.date()}.")

            tables = reader.read_parquets(prefix=prefix)
            if not tables:
                logger.warning(f"No valid parquet files for {source} on {date.date()}, skipping...")
                continue

            compacted = compact_tables(tables, target_size)

            try:
                for i, table in enumerate(compacted):
                    writer.write_partition(source, date.year, date.month, date.day, i, table)
            except Exception:
                logger.exception(f"Compaction failed for {source} on {date.date()}.")
                continue

    logger.info(f"Compacter job ended successfully at {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}. Exiting the container now...")


if __name__ == '__main__':
    main()
