import pyarrow as pa

from shared.minio.writer import BaseS3ParquetWriter


class ConsumerS3ParquetWriter(BaseS3ParquetWriter):
    def __init__(self, s3_client, bucket, file_key, schema, logger=None):
        super().__init__(s3_client, bucket, logger)
        self.file_key = file_key
        self.schema = schema

    def _build_key(self, source, year, month, day, hr, batch_id):
        return (
            f"source={source}/"
            f"year={year}/month={month}/day={day}/hr={hr:02d}/"
            f"{self.file_key}_batch_{batch_id:05d}.parquet"
        )


    def write(self, records, source, dt, hr, batch_id):
        if not records:
            return

        table = pa.Table.from_pylist(records, self.schema)

        return self.write_table(
            table,
            source=source,
            year=dt[:4],
            month=dt[5:7],
            day=dt[8:],
            hr=hr,
            batch_id=batch_id
        )

