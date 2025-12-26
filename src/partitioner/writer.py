import io
import uuid
import logging
import pyarrow.parquet as pq
from datetime import datetime

logger = logging.getLogger("trstream.partitioner.writer")

class S3Writer:

    def __init__(self, s3_client, bucket):
        self.s3 = s3_client
        self.bucket = bucket


    def _key(self, year, month, day, ttype):
        commit_time = datetime.now().strftime("%Y%m%dT%H%M%SZ")
        uid = uuid.uuid4().hex[:8]

        return (
            f"year={year}/month={month:02d}/day={day:02d}/"
            f"transaction_type={ttype}/"
            f"part-{commit_time}-{uid}.parquet"
        )


    def write_partition(self, year, month, day, ttype, table):
        key = self._key(year, month, day, ttype)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        self.s3.put_object(
            Bucket=self.bucket, 
            Key=key, 
            Body=buffer.getvalue()
        )
