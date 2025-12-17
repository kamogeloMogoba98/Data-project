
import boto3
from botocore.client import Config
import datetime 
import os

date_time= datetime.datetime.now()


class S3Mino_connection:


    def mino_connection(self, name=None, output_path=None, bucket_name=None):
        # MinIO connection
        s3 = boto3.client(
            "s3",
            endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio-server:9000"),
            aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "admin"),
            aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", "password"),
            config=Config(signature_version="s3v4")
        )

        # bucket_name = "dimjsonfiles"

        # Create bucket if it doesn't exist
        if bucket_name:
            try:
                existing_buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
                if bucket_name not in existing_buckets:
                    s3.create_bucket(Bucket=bucket_name)
                    print(f"Bucket '{bucket_name}' created!")
            except Exception as e:
                print(f"Warning: Could not check/create bucket: {e}")




        if output_path and bucket_name:
            folder = f"{name}/{date_time}" if name else "json_files"
            object_name = f"{folder}/{os.path.basename(output_path)}"
            
            try:
                s3.upload_file(output_path, bucket_name, object_name)
                print(f"Uploaded '{output_path}' to '{bucket_name}/{object_name}'")
            except Exception as e:
                print(f"Error uploading file: {e}")

        # 4. CRITICAL: Return the s3 object so other scripts can use it
        return s3


   


