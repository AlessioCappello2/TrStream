import io
import pyarrow.parquet as pq
import pyarrow as pa

class S3Reader:

    def __init__(self, s3_client, bucket, logger):
        self.s3 = s3_client
        self.bucket = bucket
        self.logger = logger

    
    def read_parquets(self, prefix: str) -> list[pa.Table]:
        """
        Read all parquet files under a given prefix and return Arrow tables.
        
        :self: S3Reader instance
        :prefix: common prefix for all parquet files to retrieve

        :return: list of Arrow tables representing the parquet files
        :rtype: list[pa.Table]
        """
        tables = []
        continuation = None

        while True:
            if continuation:
                resp = self.s3.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=prefix,
                    ContinuationToken=continuation,
                )
            else:
                resp = self.s3.list_objects_v2(
                    Bucket=self.bucket,
                    Prefix=prefix,
                )

            contents = resp.get("Contents", [])
            parquet_keys = [
                obj["Key"]
                for obj in contents
                if obj["Key"].endswith(".parquet")
            ]

            for key in parquet_keys:
                try:
                    obj = self.s3.get_object(
                        Bucket=self.bucket,
                        Key=key,
                    )
                    buf = io.BytesIO(obj["Body"].read())
                    table = pq.read_table(buf)
                    tables.append(table)
                except Exception:
                    self.logger.exception(
                        f"Failed reading parquet file: s3://{self.bucket}/{key}"
                    )

            if not resp.get("IsTruncated"):
                break

            continuation = resp.get("NextContinuationToken")

        return tables