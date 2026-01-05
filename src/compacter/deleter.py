class S3Deleter:
    def __init__(self, s3_client, bucket):
        self.s3 = s3_client
        self.bucket = bucket

    def delete_objects(self, keys):
        if not keys:
            return

        self.s3.delete_objects(
            Bucket=self.bucket,
            Delete={"Objects": [{"Key": k} for k in keys]},
        )
