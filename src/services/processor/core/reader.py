import pyarrow as pa
from shared.minio.reader import BaseS3ParquetReader

class ProcessorS3ParquetReader:
    """
    Service-specific wrapper around BaseS3ParquetReader
    for processor logic: reads all tables under a prefix.
    """

    def __init__(self, s3_client, bucket, logger):
        self.reader = BaseS3ParquetReader(s3_client, bucket, logger)

    def read_parquets(self, prefix: str) -> list[pa.Table]:
        """Accumulate all parquet tables under the given prefix."""
        all_tables = []
        continuation = None

        while True:
            keys, continuation, truncated = self.reader.list_objects_page(
                prefix=prefix,
                continuation_token=continuation
            )
            all_tables.extend(self.reader.read_parquets(keys))

            if not truncated:
                break

        return all_tables
