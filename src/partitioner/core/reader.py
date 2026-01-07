import io
import pyarrow.parquet as pq

class S3Reader:

    def __init__(self, s3_client, bucket):
        self.s3 = s3_client
        self.bucket = bucket


    def list_objects_page(self, continuation_token=None):
        kwargs = {"Bucket": self.bucket}
        if continuation_token:
            kwargs["ContinuationToken"] = continuation_token

        resp = self.s3.list_objects_v2(**kwargs)

        keys = [obj["Key"] for obj in resp.get("Contents", [])]
        return (
            keys,
            resp.get("NextContinuationToken"),
            resp.get("IsTruncated", False),
        )


    def read_parquets(self, keys):
        tables = []

        for key in keys:
            if not key.endswith(".parquet"):
                continue

            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = obj["Body"].read()

            if body: # consider only non-empty files
                tables.append(pq.read_table(io.BytesIO(body)))

        return tables
