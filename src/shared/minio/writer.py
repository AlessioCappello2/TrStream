import io
import logging
import pyarrow.parquet as pq

class BaseS3ParquetWriter:
    def __init__(self, s3_client, bucket, logger=None):
        self.s3 = s3_client
        self.bucket = bucket
        self.logger = logger or logging.getLogger(__name__)

    def _build_key(self, **kwargs) -> str:
        raise NotImplementedError

    def write_table(self, table, **key_params):
        buffer = io.BytesIO()
        pq.write_table(table, buffer)
        buffer.seek(0)

        key = self._build_key(**key_params)

        try:
            self.s3.put_object(
                Bucket=self.bucket,
                Key=key,
                Body=buffer.getvalue()
            )
            self.logger.info(f"Uploaded {key}")
        except Exception:
            self.logger.exception(
                f"Failed uploading s3://{self.bucket}/{key}"
            )
            raise

        return key
