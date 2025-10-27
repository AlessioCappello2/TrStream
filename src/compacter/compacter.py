####################################################################
# IMPORTS #
####################################################################
import io
import os
import uuid
import boto3
import math
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from datetime import datetime, timedelta

####################################################################
# Env variables
####################################################################
minio_endpoint = os.environ['MINIO_ENDPOINT']
access_key = os.environ['MINIO_ACCESS_KEY']
secret_key = os.environ['MINIO_SECRET_KEY']
bucket_name = 'tb-transactions'
target_size = 256 * 1024 # 256 MB expressed in Bytes


def generate_output_key(ttype: str, index: int):
    return f"year={year}/month={month:02d}/day={day:02d}/transasction_type={ttype}/part-{index:05d}.parquet"


if __name__ == '__main__':
    s3 = boto3.client('s3', endpoint_url=minio_endpoint, aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    target_date = datetime.now() - timedelta(1)  # previous day
    day, month, year = target_date.day, target_date.month, target_date.year
    
    objects = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'year={year}/month={month:02d}/day={day:02d}/').get('Contents', [])

    for ttype in ["CREDIT", "DEBIT", "REFUND"]:
        processed = []
        parquets = [obj for obj in objects if f"transaction_type={ttype}" in obj['Key']]

        total_size = sum(obj['Size'] for obj in parquets)
        num_files = math.ceil(total_size / target_size)
        
        tables = []
        for p in parquets:
            key = p['Key']
            response = s3.get_object(Bucket=bucket_name, Key=key)
            data = io.BytesIO(response["Body"].read())
            tables.append(pq.read_table(data))
            processed.append({'Key': key})

        table = pa.concat_tables(tables)
        num_rows = table.num_rows # sum(row_group.num_rows for fragment in table.get_fragments() for row_group in fragment)

        rows_per_file = math.ceil(num_rows / num_files)

        for i in range(num_files):
            start, end = i*rows_per_file, min((i+1)*rows_per_file, num_rows)
            file_table = table.slice(start, end-start)

            buffer = io.BytesIO()
            pq.write_table(file_table, buffer)
            buffer.seek(0)

            key = generate_output_key(ttype=ttype, index=i+1)
            s3.put_object(Bucket=bucket_name, Key=key, Body=buffer.getvalue())

        s3.delete_objects(Bucket=bucket_name, Delete={'Objects': processed})