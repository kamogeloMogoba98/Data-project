import duckdb
import os




class duckconnect():

    def __init__(self, db_path="my_local.db"):
        self.db_path = db_path
        self.con = None
    

    def duckconnector(self):

        self.db_path = "/opt/airflow/airflow_home_dev/my_local.db"

        # 2. SAFETY CHECK: Ensure the folder exists before connecting
        # If this folder is missing, DuckDB will fail.
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)


        self.con = duckdb.connect(self.db_path)
        # 1. Install and Load HTTPFS
        self.con.execute("INSTALL httpfs;")
        self.con.execute("LOAD httpfs;")

        # 2. Use the modern configuration method (duckdb.sql())

        self.con.sql("SET s3_endpoint='minio-server:9000';")
        self.con.sql("SET s3_access_key_id='minioadmin';")
        self.con.sql("SET s3_secret_access_key='minioadmin';")
        self.con.sql("SET s3_use_ssl=false;") # Set to false since you are using HTTP (no SSL)

        # OPTIONAL: This is often necessary for MinIO
        self.con.sql("SET s3_url_style='path';")

        return self.con