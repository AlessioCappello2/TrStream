import io
import pyarrow.parquet as pq
import pyarrow as pa
from typing import List, Tuple, Optional


class BaseS3ParquetReader:

    def __init__(self, s3_client, bucket: str, logger=None):
        self.s3 = s3_client
        self.bucket = bucket
        self.logger = logger

    # ---------------------------------
    # Pagination only
    # ---------------------------------
    def list_objects_page(
        self,
        prefix: Optional[str] = None,
        continuation_token: Optional[str] = None
    ) -> Tuple[List[str], Optional[str], bool]:

        kwargs = {"Bucket": self.bucket}

        if prefix:
            kwargs["Prefix"] = prefix

        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = self.s3.list_objects_v2(**kwargs)

        keys = [obj["Key"] for obj in resp.get("Contents", [])]

        return (
            keys,
            resp.get("NextContinuationToken"),
            resp.get("IsTruncated", False),
        )

    # ---------------------------------
    # Single parquet read
    # ---------------------------------
    def read_parquet(self, key: str) -> Optional[pa.Table]:
        if not key.endswith(".parquet"):
            return None

        try:
            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = obj["Body"].read()

            if not body:
                return None

            return pq.read_table(io.BytesIO(body))

        except Exception:
            if self.logger:
                self.logger.exception(
                    f"Failed reading parquet file: s3://{self.bucket}/{key}"
                )
            return None

    # ---------------------------------
    # Batch read (no pagination)
    # ---------------------------------
    def read_parquets(self, keys: List[str]) -> List[pa.Table]:
        tables = []

        for key in keys:
            table = self.read_parquet(key)
            if table:
                tables.append(table)

        return tables