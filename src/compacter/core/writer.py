import io 
import pyarrow.parquet as pq


class S3Writer:
    
    def __init__(self, s3_client, bucket):
        self.s3 = s3_client
        self.bucket = bucket

    def write_compacted_partition(self, year, month, day, ttype, index, table):
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        key = (
            f"year={year}/month={month:02d}/day={day:02d}/"
            f"transaction_type={ttype}/"
            f"compact-{index:06d}.parquet"
        )

        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
