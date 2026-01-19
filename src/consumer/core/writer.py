import io
import pyarrow as pa
import pyarrow.parquet as pq
import logging

logger = logging.getLogger("trstream.consumer.writer")

class ParquetS3Writer:

    def __init__(self, s3_client, bucket, file_key, schema):
        self.s3 = s3_client
        self.bucket = bucket
        self.file_key = file_key
        self.schema = schema

    def write(self, records, source, dt, batch_id):
        if not records:
            return

        table = pa.Table.from_pylist(records, self.schema)
        buffer = io.BytesIO()
        pq.write_table(table, buffer)

        key = f"source={source}/year={dt[:4]}/month={dt[5:7]}/day={dt[8:]}/{self.file_key}_batch_{batch_id:07d}.parquet"
        self.s3.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=buffer.getvalue()
        )

        logger.info(
            f"Uploaded batch {batch_id} containing {len(records)} records."
        )
