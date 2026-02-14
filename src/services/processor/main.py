####################################################################
# IMPORTS #
####################################################################
import boto3
import pyarrow as pa
from datetime import datetime
from pathlib import Path

from processor.config.settings import settings
from processor.config.dates import resolve_dates
from shared.config.logging_config import setup_logging
from shared.config.load_config import load_config_from_directory

from processor.core.reader import ProcessorS3ParquetReader 
from processor.core.writer import ProcessorS3ParquetWriter
from processor.core.processing import normalize_faker, normalize_stripe, normalize_revolut

####################################################################
# Logging
####################################################################
logger = setup_logging("trstream.processor")

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
        
    logger.info(f"Processor job started at: {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}")
    
    ####################################################################
    # Config reading
    #################################################################### 
    cfg = load_config_from_directory(Path("src"), "processor.yaml")
    dates = resolve_dates(cfg)
    sources = cfg['process']['sources']

    ####################################################################
    # S3 client, S3Reader and S3Writer instantiation
    ####################################################################
    s3 = boto3.client(
        's3', 
        endpoint_url=minio_endpoint, 
        aws_access_key_id=access_key, 
        aws_secret_access_key=secret_key
    )

    reader = ProcessorS3ParquetReader(
        s3_client=s3,
        bucket=bucket_src,
        logger=logger
    )

    writer = ProcessorS3ParquetWriter(
        s3_client=s3,
        bucket=bucket_trg,
        logger=logger
    )

    ####################################################################
    # Processing logic
    ####################################################################
    for date in dates:
        for source in sources:

            daily_tables = []

            # Process every hour of the day
            for hour in range(24): 
                prefix = (
                    f"source={source}/"
                    f"year={date.year}/month={date.month:02d}/day={date.day:02d}/"
                    f"hr={hour:02d}/"
                )

                tables = reader.read_parquets(prefix=prefix)
                if not tables:
                    continue

                table = pa.concat_tables(tables)

                if source == "faker":
                    table = normalize_faker(table)
                elif source == "stripe":
                    table = normalize_stripe(table)
                elif source == "revolut":
                    table = normalize_revolut(table)

                if table.num_rows == 0:
                    continue

                daily_tables.append(table)
            
            if not daily_tables:
                logger.info(f"No data for source={source}, date={date.date()}")
                continue

            # Write daily partition
            final_table = pa.concat_tables(daily_tables)
            writer.write_partition(
                source=source,
                year=date.year,
                month=date.month,
                day=date.day,
                table=final_table
            )

    logger.info(f"Processor job ended successfully at {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}. Exiting the container now...")


if __name__ == '__main__':
    main()
