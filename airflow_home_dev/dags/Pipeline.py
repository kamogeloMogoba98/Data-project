# """"
# This is a pipeline to move data from source through different landing zone(bronze, silver and gold) in the minio S3 object storage application as parquet and json files
# We then create table from these files in the duckdb into the different sections (bronze,silver and gold).
# We execute using methods from varies scripts


import sys
import os
sys.path.append(os.path.join(os.environ.get("airflow_home_dev", "."), "dags"))
import logging
from datetime import datetime, timedelta

# Airflow Imports
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.datasets import Dataset

from DimGenerators.DimDataGenerator import dimGeneratorData
# from DimGenerators.DimPromotion import  dimPromotionGen
# from DimGenerators.DimProduct import dimProductGen
from table_insert.BronzeTable import Bronzetables
from table_insert.Silvertables import dimDataParquert, Silvertables
from table_insert.Goldtable import GoldDataParquet, GoldFactTable

# Create a logger
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="Pipeline",
    start_date=datetime(2021, 1, 1),
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    doc_md="""
    # Sales Delta Pipeline
    **Architecture**: Bronze → Silver → Gold (Medallion)
    **Flow**:
    1. **Bronze**: Ingest raw sales data
    2. **Silver**: Clean, validate
    3. **Gold**: Aggregate
    """
)
def sale_process_pipeline():

    # 1. Define a Start Task
    start = EmptyOperator(task_id="start_pipeline")

    # 2. Define the Python Task
    @task(
        task_id="insert_files_into_bronze_s3",
        outlets=[Dataset("s3://bronze/sales_raw/")] 
    )
    def insert_into_bronze_s3_files():
        """
        The data from the source is sent to an S3 bucket in MinIO as a JSON file.
        """
        try:
            bronze_DimCustomer = dimGeneratorData()
            # Assuming this returns a dictionary or path
            bronze_Customer = bronze_DimCustomer.dimCustomerGenerator()

            
            logger.info(
                f"The customer data from source has been ingested. ",
                f"Record count: {bronze_Customer['count']}, ",
                f"Destination file: {bronze_Customer['Output_path']}."
            )

            bronze_DimProduct = dimGeneratorData()
            # Assuming this returns a dictionary or path
            bronze_Product = bronze_DimProduct.dimProductGenerator()

            
            logger.info(
                f"The product data from source has been ingested. ",
                f"Record count: {bronze_Product['count']}, ",
                f"Destination file: {bronze_Product['Output_path']}."
            )


            bronze_DimPromotion = dimGeneratorData()
            # Assuming this returns a dictionary or path
            bronze_Promotion = bronze_DimPromotion.dimPromotiontGenerator()

            
            logger.info(
                f"The product data from source has been ingested. ",
                f"Record count: {bronze_Promotion['count']}, ",
                f"Destination file: {bronze_Promotion['Output_path']}."
            )



            return bronze_Customer, bronze_Product, bronze_Promotion

        except Exception as e:
            logger.error(f"S3 bronze files failed: {e}")
            raise

    @task(
        task_id="S3_json_to_bronzetable",
        outlets=[Dataset("s3://bronze/sales_raw/")] 
    )
    def S3_jsonfiles_to_bronzetable():
         
        try:
            bronze_DimProm= Bronzetables()
            # Assuming this returns a dictionary or path
            DimProm_table = bronze_DimProm.BronzePromotiontable()

            
            logger.info(
                f" {DimProm_table['status_3']}. ",
                f"Record count: {DimProm_table['count']}, "
                
            )

            bronze_DimProduct = Bronzetables()
            # Assuming this returns a dictionary or path
            DimProd_table = bronze_DimProduct.BronzeDim_Product()

            
            logger.info(
                f" {DimProd_table['status_3']}. ",
                f"Record count: {DimProd_table['count']}, "
            )


            bronze_DimCustomer = Bronzetables()
            # Assuming this returns a dictionary or path
            DimCustomer_table = bronze_DimCustomer.BronzeCustomertable()

            
            logger.info(
                f"{DimProd_table['status_3']}.",
                f"Record count: {DimCustomer_table['count']}, "
               
            )



            return DimProm_table, DimProd_table, DimCustomer_table

        except Exception as e:
            logger.error(f"Bronze ingestion failed: {e}")
            raise


      #bronze to silver parquet files
    @task(
        task_id="bronzetable_to_silverParquetfiles",
        outlets=[Dataset("s3://bronze/sales_raw/")] 
    )

    def bronzetable_to_silverParquet():
       
        try:
            DataparquetProm=dimDataParquert()
            # Assuming this returns a dictionary or path
            PromoParquetfile = DataparquetProm.DimPromotionTypeParquert()
    
            logger.info(
                f"The record count{PromoParquetfile['count']}.",
                f"Output_path: {PromoParquetfile['Output']}, "
            )

            DataparquetCustomer= dimDataParquert()
            CustParquetfile = DataparquetCustomer.DimCustomerParquert()

            logger.info(
                f"The record count{CustParquetfile['count']}.",
                f"Output_path: {CustParquetfile['Output']}, "
            )
            DataparquetProduct= dimDataParquert()
            ProdParquetfile = DataparquetProduct.DimProductParquet()

            logger.info(
                f"The record count{ProdParquetfile['count']}.",
                f"Output_path: {ProdParquetfile['Output']}, "
            )


            return PromoParquetfile, CustParquetfile, ProdParquetfile

        except Exception as e:
            logger.error(f"Silver parquet ingestion failed: {e}")
            raise

    @task(
        task_id="silverParquetfiles_silverDimTable",
        outlets=[Dataset("s3://bronze/sales_raw/")] 
    )

    def silverParquetfiles_silverDimTable():

        try:

            SilverPromTable=Silvertables()
            # Assuming this returns a dictionary or path
            SilverDimPromTable= SilverPromTable.dimPromotionSilverTable()
    
            logger.info(
                f"The record count{SilverDimPromTable['count']}.",
                f"Output_path: {SilverDimPromTable['Output']}, "
            )

            SilverCustomTable= Silvertables()
            SilverDimCustomTable = SilverCustomTable.dimCustomerSilverTable()

            logger.info(
                f"The record count{SilverDimCustomTable['count']}.",
                f"Output_path: {SilverDimCustomTable['Output']}, "
            )
            
            SilverProductTable= Silvertables()
            SilverDimProductTable = SilverProductTable.dimProductSilverTable()

            logger.info(
                f"The record count{SilverDimProductTable['count']}."
                f"Output_path: {SilverDimProductTable['Output']}, "
            )


        except Exception as e:
            logger.error(f"Silver parquet ingestion failed: {e}")
            raise
 

    @task(
        task_id="Goldfile_to_goldfacttable",
        outlets=[Dataset("s3://bronze/sales_raw/")] 
    )

    def Goldparquet_to_facttable():


        try:
            GoldParquet=GoldDataParquet()
            # Assuming this returns a dictionary or path
            GoldSalesparquetfile= GoldParquet.GoldSalesparquet()
    
            logger.info(
                f"The record count{GoldSalesparquetfile['count']}.",
                f"Output_path: {GoldSalesparquetfile['Output']}, "
            )

            GoldFactTables=GoldFactTable()
            # Assuming this returns a dictionary or path
            GoldFactSalestable= GoldFactTables.GoldFactSales()
    
            logger.info(
                f"The record count{GoldFactSalestable['count']}.",
                f"Output_path: {GoldFactSalestable['Output']}, "
            )
    
        
        except Exception as e:
            logger.error(f"There is an issue with the gold injestion:{e}")
            raise
    


    
    @task
    def push_to_motherduck():
        import duckdb
        import os
        
        local_path = "/opt/airflow/airflow_home_dev/my_local.db"
        token = os.getenv('MOTHERDUCK_TOKEN')
        
        # 1. Connect to the local file
        con = duckdb.connect(local_path)
        
        try:
            # 2. Attach MotherDuck
            con.sql("LOAD motherduck;")
            con.sql(f"SET motherduck_token='{token}';")
            con.sql("CREATE DATABASE IF NOT EXISTS my_database;")

            con.sql("ATTACH 'md:my_database' AS cloud_db;")
            
            # 3. Get a list of all tables in the local DB
            # This query ignores internal DuckDB system tables
            tables = con.sql("""
                SELECT table_schema, table_name 
                FROM information_schema.tables 
                WHERE table_schema NOT IN ('information_schema', 'pg_catalog', 'main')
            """).fetchall()

            for schema_name, table_name in tables:
                print(f"Syncing {schema_name}.{table_name} to MotherDuck...")
                
                # Ensure the schema exists in the cloud first
                con.sql(f"CREATE SCHEMA IF NOT EXISTS cloud_db.{schema_name};")
                
                # 🚀 Mirror the table exactly (keeps names, columns, and data)
                con.sql(f"""
                    CREATE OR REPLACE TABLE cloud_db.{schema_name}.{table_name} AS 
                    SELECT * FROM {schema_name}.{table_name};
                """)
                
            print(f"Successfully synced {len(tables)} tables to MotherDuck!")

        except Exception as e:
            print(f"Sync failed: {e}")
            raise e
        finally:
            con.close()

  

    # 3. Call the task function to create the task instance
    injest_json_bronze = insert_into_bronze_s3_files()
    bronzeTables=S3_jsonfiles_to_bronzetable()
    silverParquet =bronzetable_to_silverParquet()
    SilverDimTables=silverParquetfiles_silverDimTable()
    Goldoarquet_to_GoldFacttables=Goldparquet_to_facttable()
    # Assuming your ingestion task is called ingest_task
    motherduck=push_to_motherduck()


    # 4. Set Dependencies
    start >>injest_json_bronze>>bronzeTables>>silverParquet>>SilverDimTables>>Goldoarquet_to_GoldFacttables>>motherduck

# 5. Instantiate the DAG
dag_instance = sale_process_pipeline()