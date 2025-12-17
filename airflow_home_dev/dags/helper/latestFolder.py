
from helper.MinoS3_connection import S3Mino_connection
from datetime import datetime



mino= S3Mino_connection()

timestamp = datetime.now()

s3= mino.mino_connection()


class lastestFolder:


   def lastestinstance(self, Buckname=None, sub_dirc=None):
        


       
        sub_dirc = sub_dirc.rstrip('/')

        # List all files
        # We add the slash here ensuring we look inside the folder
        response = s3.list_objects_v2(Bucket=Buckname, Prefix=f'{sub_dirc}/')

        timestamp_folders = set()

        if 'Contents' not in response:
                return None

        if 'Contents' in response:
            for obj in response['Contents']:
                # Example Key: dim_promotion/2025-12-13 17:25:38/data.parquet
                
                # 2. Logic Fix: Replace with slash to avoid leading empty strings
                # Result: "2025-12-13 17:25:38/data.parquet"
                relative_path = obj['Key'].replace(f'{sub_dirc}/', '')
                
                # Get the first part (the timestamp folder)
                folder_name = relative_path.split('/')[0]
                
                
                if folder_name and folder_name != ".":
                    timestamp_folders.add(folder_name)

        if not timestamp_folders:
            print(f"No folders found inside s3://{Buckname}/{sub_dirc}/")
            return None

        # Sort chronologically
        latest_folder = sorted(list(timestamp_folders))[-1]

        # 3. Fix Return Value: Return the FULL PATH for DuckDB
        final_path = f"s3://{Buckname}/{sub_dirc}/{latest_folder}/*.parquet"

        print(f"Found latest data: {final_path}")
        return latest_folder 
       


