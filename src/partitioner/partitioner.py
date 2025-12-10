####################################################################
# IMPORTS #
####################################################################
import io
import os
import uuid
import boto3
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from datetime import datetime
from dotenv import load_dotenv

####################################################################
# Env variables
####################################################################
load_dotenv()

bucket_src = os.getenv('BUCKET_SRC', 'raw-data')
bucket_trg = os.getenv('BUCKET_TRG', 'tb-transactions')

minio_endpoint = os.environ['MINIO_ENDPOINT']
access_key = os.environ['MINIO_ACCESS_KEY']
secret_key = os.environ['MINIO_SECRET_KEY']

# CONSTANT
BATCH_SIZE = 10

####################################################################
# Helper function for generating a Parquet file key
####################################################################
def generate_parquet_key(year, month, day, ttype):
    commit_time = datetime.now().strftime("%Y%m%dT%H%M%SZ")
    file_uuid = uuid.uuid4().hex[:8]
    file_key = f"part-{commit_time}-{file_uuid}.parquet"

    return f"year={year}/month={month:02d}/day={day:02d}/transaction_type={ttype}/{file_key}"


if __name__ == '__main__':
    
    print(f"Partitioner job started at: {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}", flush=True)
    
    ####################################################################
    # S3 client and files retrieval from the ingestion bucket
    ####################################################################
    s3 = boto3.client('s3', endpoint_url=minio_endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    
    # Overcome the limitation of list_objects_v2 that retrieves up to 1000 objects
    while True:
        objects = s3.list_objects_v2(Bucket=bucket_src).get('Contents', [])
        if not objects:
            break

        parquets = [obj['Key'] for obj in objects if obj['Key'].endswith('.parquet')]
        tables, processed, i, n = [], [], 0, len(parquets)

        ####################################################################
        # Process the raw parquets and partition them into the tb bucket
        ####################################################################   
        for key in parquets:

            response = s3.get_object(Bucket=bucket_src, Key=key)
            body = response["Body"].read()
            if len(body) > 0:  # consider only non-empty files
                data = io.BytesIO(body)
                tables.append(pq.read_table(data))
            processed.append({'Key': key})
            i += 1

            if not i % BATCH_SIZE or i == n:  # accumulate 
                table = pa.concat_tables(tables)

                # Cast string to timestamp
                table = table.set_column(
                    table.schema.get_field_index("timestamp"),
                    "timestamp",
                    pc.strptime(
                        pa.array([s.split('.')[0] for s in table["timestamp"].to_pylist()]), 
                        format="%Y-%m-%dT%H:%M:%S", 
                        unit="s"
                    )
                )

                # Partition keys
                timestamps = table.column("timestamp")
                transaction_types = table.column("transaction_type")

                for ttype in set(transaction_types.to_pylist()):
                    # Filter table based on trasaction type
                    filtered_type = table.filter(pc.field("transaction_type") == ttype)
                    dates = [ts.date() for ts in timestamps.to_pylist()]

                    for date in set(dates):
                        # Filter transactions based on date
                        filtered_date = filtered_type.filter(pc.day(pc.field("timestamp")) == date.day)

                        # Add missing partition fields and generate Parquet
                        year, month, day = date.year, date.month, date.day
                        filtered_date.append_column('year', pa.array([str(year)] * filtered_date.num_rows, pa.string()))
                        filtered_date.append_column('month', pa.array([f"{month:02d}"] * filtered_date.num_rows, pa.string()))
                        filtered_date.append_column('day', pa.array([f"{day:02d}"] * filtered_date.num_rows, pa.string()))                    
                        output_key = generate_parquet_key(year=year, month=month, day=day, ttype=ttype)

                        buffer = io.BytesIO()
                        pq.write_table(filtered_date, buffer)
                        buffer.seek(0)

                        s3.put_object(Bucket=bucket_trg, Key=output_key, Body=buffer.getvalue())

                s3.delete_objects(Bucket=bucket_src, Delete={'Objects': processed})
                tables, processed = [], []

    print(f"Partitioner job ended successfully at {datetime.now().isoformat(timespec='seconds').replace('T', ' ')}. Exiting the container now...", flush=True)