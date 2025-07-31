import boto3

class S3Uploader:
    def __init__(self, config):
        self.s3_client = boto3.client(
            "s3",
            region_name=config["s3"]["region"],
            aws_access_key_id=config["s3"]["aws_access_key_id"],
            aws_secret_access_key=config["s3"]["aws_secret_access_key"]
        )
        self.bucket = config["s3"]["bucket"]

    def upload_to_s3(self, local_file, s3_key):
        """Upload Parquet file to S3."""
        try:
            self.s3_client.upload_file(local_file, self.bucket, s3_key)
            print(f"Uploaded Parquet file to s3://{self.bucket}/{s3_key}")
        except Exception as e:
            print(f"Error uploading to S3: {e}")
            raise

    def run(self, local_file, s3_key):
        """Run the S3 upload process."""
        self.upload_to_s3(local_file, s3_key)

if __name__ == "__main__":
    # Test the class
    config = {
        "s3": {
            "bucket": "poc-bucket-2102",
            "region": "us-east-1",
            "aws_access_key_id": "your_access_key_id",
            "aws_secret_access_key": "your_secret_access_key"
        }
    }
    uploader = S3Uploader(config)
    uploader.run("test.parquet", "tickets/test.parquet")