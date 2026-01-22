####################################################################
# IMPORTS #
####################################################################
import boto3
import pyarrow as pa
from datetime import datetime

from processor.config.settings import settings
from processor.config.logging_config import setup_logging
from processor.config.load_config import load_config, resolve_dates

from processor.core.reader import S3Reader 
from processor.core.writer import S3Writer
from processor.core.processing import normalize_faker, normalize_stripe

####################################################################
# Logging
####################################################################
logger = setup_logging()

####################################################################
# Env variables
####################################################################
bucket_src = settings.minio_ingestion_bucket
bucket_trg = settings.minio_processed_bucket

minio_endpoint = settings.minio_endpoint
access_key = settings.minio_access_key
secret_key = settings.minio_secret_key

# Main function
def main():
        
    logger.info(f"Processor job started at: {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}")
    
    ####################################################################
    # Config reading
    #################################################################### 
    cfg = load_config()
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

    reader = S3Reader(
        s3_client=s3,
        bucket=bucket_src,
        logger=logger
    )

    writer = S3Writer(
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
