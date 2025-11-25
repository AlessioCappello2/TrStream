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

####################################################################
# Env variables
####################################################################
minio_endpoint = os.environ['MINIO_ENDPOINT']
access_key = os.environ['MINIO_ACCESS_KEY']
secret_key = os.environ['MINIO_SECRET_KEY']
bucket_src = 'raw-data'
bucket_trg = 'tb-transactions'

# CONSTANT
BATCH_SIZE = 10

def generate_parquet_key():
    commit_time = datetime.now().strftime("%Y%m%dT%H%M%SZ")
    file_uuid = uuid.uuid4().hex[:8]
    file_key = f"part-{commit_time}-{file_uuid}.parquet"

    return f"year={year}/month={month:02d}/day={day:02d}/transaction_type={ttype}/{file_key}"

if __name__ == '__main__':
    s3 = boto3.client('s3', endpoint_url=minio_endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    objects = s3.list_objects_v2(Bucket=bucket_src).get('Contents', [])
    parquets = [obj['Key'] for obj in objects if obj['Key'].endswith('.parquet')]
    tables, processed, i, n = [], [], 0, len(parquets)

    for key in parquets:

        response = s3.get_object(Bucket=bucket_src, Key=key)
        body = response["Body"].read()
        if len(body) > 0:
            data = io.BytesIO(body)
            tables.append(pq.read_table(data))
        processed.append({'Key': key})
        i += 1

        if not i % BATCH_SIZE or i == n:
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
                    filtered_date = table.filter(pc.day(pc.field("timestamp")) == date.day)

                    year, month, day = date.year, date.month, date.day
                    output_key = generate_parquet_key()

                    buffer = io.BytesIO()
                    pq.write_table(filtered_date, buffer)
                    buffer.seek(0)

                    s3.put_object(Bucket=bucket_trg, Key=output_key, Body=buffer.getvalue())

            s3.delete_objects(Bucket=bucket_src, Delete={'Objects': processed})
            tables, processed = [], []