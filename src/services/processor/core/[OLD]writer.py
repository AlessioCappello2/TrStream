import io
import uuid
import logging
import pyarrow.parquet as pq
from datetime import datetime

logger = logging.getLogger("trstream.partitioner.writer")

class S3Writer:

    def __init__(self, s3_client, bucket, logger):
        self.s3 = s3_client
        self.bucket = bucket
        self.logger = logger


    def _key(self, source: str, year: str, month: str, day: str) -> str:
        """
        Build the file key given the input parameters.
        
        :param self: S3Writer instance
        :param source: source
        :param year: year
        :param month: month
        :param day: day

        :return: file key
        :rtype: str
        """
        commit_time = datetime.now().strftime("%Y%m%dT%H%M%SZ")
        uid = uuid.uuid4().hex[:8]

        return (
            f"source={source}/"
            f"year={year}/month={month:02d}/day={day:02d}/"
            f"part-{commit_time}-{uid}.parquet"
        )


    def write_partition(self, source, year, month, day, table):
        """
        Write a partition to S3.

        :param self: S3Writer instance
        :param source: source
        :param year: year
        :param month: month
        :param day: day

        :return: file key
        :rtype: str
        """
        key = self._key(source, year, month, day)

        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        try:
            self.s3.put_object(
                Bucket=self.bucket, 
                Key=key, 
                Body=buffer.getvalue()
            )
        except Exception as e:
            self.logger.error(f"Partition {key} failed to upload: {e}")
