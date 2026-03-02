import boto3
from dotenv import load_dotenv
import os
from botocore.exceptions import ClientError

class MinioManager():
    def __init__(self):
        load_dotenv()
        access_key = os.getenv("MINIO_ACCESS_KEY")
        secret_key = os.getenv("MINIO_SECRET_KEY")

        self.s3_client = boto3.client(
        "s3",
        endpoint_url="http://localhost:9000",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
        )

    def init_bucket(self, bucket_name):
        try:
            self.s3_client.head_bucket(Bucket=bucket_name)
            print(f"User '{bucket_name}' found")
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == '404':
                print(f"Bucket '{bucket_name}' doesnt exists. Creating it...")
                self.s3_client.create_bucket(Bucket=bucket_name)
                print(f"Bucket '{bucket_name}' created succesfully.")
            else:
                print(f"Unexpected error: {e}")

    def upload_image(self,username,path_to_image):
        self.init_bucket(bucket_name=username)
        self.s3_client.upload_file(path_to_image, username, "image1")
