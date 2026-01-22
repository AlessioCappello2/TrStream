import io
import pyarrow.parquet as pq

class S3Reader:

    def __init__(self, s3_client, bucket):
        self.s3 = s3_client
        self.bucket = bucket


    def list_objects_page(self, prefix=None, continuation_token=None):
        """
        Retrieve file keys given a certain prefix and continuation token to keep track of the pagination.
        
        :param self: S3Reader instance
        :param prefix: common prefix to files to retrieve
        :param continuation_token: continuation token for pagination
        """
        
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


    def read_parquets(self, keys):
        """
        Return a list of Arrow tables given a list of file keys.
        
        :param self: S3Reader instance
        :param keys: keys to retrieve
        """
        tables = []

        for key in keys:
            if not key.endswith(".parquet"):
                continue

            obj = self.s3.get_object(Bucket=self.bucket, Key=key)
            body = obj["Body"].read()

            if body: # consider only non-empty files
                tables.append(pq.read_table(io.BytesIO(body)))

        return tables
