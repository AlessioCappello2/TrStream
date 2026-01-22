import io 
import pyarrow as pa
import pyarrow.parquet as pq


class S3Writer:
    
    def __init__(self, s3_client, bucket):
        self.s3 = s3_client
        self.bucket = bucket


    def write_compacted_partition(self, source: str, year: str, month: str, day: str, index: int, table: pa.Table):
        """
        Write a partition to S3.

        :param self: S3Writer instance
        :param source: source
        :param year: year
        :param month: month
        :param day: day
        :param index: index of the partition
        """
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        key = (
            f"tb-{source}/"
            f"year={year}/month={month:02d}/day={day:02d}/"
            f"compact-{index:06d}.parquet"
        )
        
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
