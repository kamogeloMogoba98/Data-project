#we going to do schema validation.
#and record counts  making sure we can use it in airflow.
#in this ductDb we also want to have different zones
#One dataset for bronze , there other for Silver so forth and so on...


import duckdb
import pandas as pd 
# from datatime import timestamp
from datetime import datetime
from helper.duckdbconc import duckconnect
from helper.MinoS3_connection import S3Mino_connection
from helper.latestFolder import lastestFolder


latest= lastestFolder()



#connecting to the databes
#we can modulise this section of the code

# duckdb =duckconnect()

mino= S3Mino_connection()
# con=duckdb.duckconnector()
timestamp = datetime.now()

class Bronzetables:


    def BronzePromotiontable (self):
        duckdb =duckconnect()
        con=duckdb.duckconnector()
        

        #drop the table
        drop_table_sql = """
        CREATE SCHEMA IF NOT EXISTS bronze;
        drop table IF EXISTS  bronze.PromotionType;
        """
        con.sql(drop_table_sql)
        print("Table 'PromotionType' ensured to is dropped")


        #schema  creation table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS bronze.PromotionType (
            PromotionID VARCHAR PRIMARY KEY,  -- PK
            ProductID VARCHAR,              -- FK
            PromotionName VARCHAR,
            DiscountPercent DOUBLE,         -- Use DOUBLE for floating point numbers
            StartDate TIMESTAMP,            -- Use TIMESTAMP for datetime objects
            EndDate TIMESTAMP,
            PromotionType VARCHAR
        );
        """
        con.sql(create_table_sql)
        


        # 3. Use s3:// protocol

        # --- B. Load Data from MinIO JSON File ---
        # We use the COPY command or the INSERT INTO SELECT pattern for loading data.
        # Note: read_json_auto infers the schema from the file and maps it to the table schema.
        latest_file = latest.lastestinstance(Buckname="bronze", sub_dirc="dim_promotion")
     



        load_data_sql = f"""
        INSERT INTO bronze.PromotionType

        SELECT PromotionID,
            ProductID,
            PromotionName,
            DiscountPercent,
            StartDate,
            EndDate,
            PromotionType
            FROM read_json_auto('s3://bronze/dim_promotion/{latest_file}/*.json')
        ;
        """

        table_count = con.sql(load_data_sql)


        data_count  = f"""

        SELECT 
        *
        FROM bronze.PromotionType;
        """

        table_count_1 = con.sql(data_count)

        rows_inserted = 0
        try:
            # Execute the load command
            result = con.sql(load_data_sql)
            rows_inserted = con.sql("SELECT COUNT(*) FROM bronze.PromotionType").fetchone()[0]
            
            # --- C. Verification ---
            verification_df = con.sql("SELECT * FROM bronze.PromotionType LIMIT 5").df()
            
            print(f"\n Data Load Successful! Inserted {rows_inserted} rows.")
            print("\n--- Verified Data Sample ---")
            print(verification_df)
            
        except Exception as e:
            print(f"\nERROR during data loading: {e}")

        return  {
                    "count": rows_inserted,
                    "timestamp": timestamp.isoformat(),
                    "status_1":  "Table 'PromotionType' ensured to is dropped",
                    "status_2":  "Table 'PromotionType' ensured to is createted",
                    "status_3":  "Table 'bronze.PromotionType' is created ",
                }
        con.close()





    ############################################
    #We going to add the DimCustomer here
    #######################################


    def BronzeCustomertable (self):
        duckdb =duckconnect()
        con=duckdb.duckconnector()
       


        #schema  creation table
        create_table_sql_dim_Customer = """

        drop table IF EXISTS  Bronze.Dim_Customer;
        CREATE SCHEMA IF NOT EXISTS bronze;
        
        CREATE TABLE IF NOT EXISTS bronze.Dim_Customer (
            CustomerID VARCHAR PRIMARY KEY,    -- Primary Key
            FirstName VARCHAR,
            LastName VARCHAR,
            Email VARCHAR,
            PhoneNumber VARCHAR,
            ProductID VARCHAR,                 -- Foreign Key column

        );
        """
        con.sql(create_table_sql_dim_Customer)
        print("Table 'PromotionType' ensured to exist.")
        latest_file = latest.lastestinstance(Buckname="bronze", sub_dirc="dim_Customers")


        load_data_sql = f"""
        INSERT INTO bronze.Dim_Customer

            SELECT 
                CustomerID, 
                FirstName, 
                LastName, 
                Email,
                PhonenUmber,
                ProductID
            FROM read_json_auto('s3://bronze/dim_Customers/{latest_file}/*.json');
        """

        table_count = con.sql(load_data_sql)
        print("kamo check if the table loads")
        print("I am going to add checks here,using this")
        print(table_count)


        data_count  = f"""
        SELECT 
        *
        FROM bronze.Dim_Customer;
        """

        table_count_1 = con.sql(data_count)
        print("kamo check if the is a certain amount in the table")
        print(table_count_1)

        rows_inserted = 0
        try:
            # Execute the load command
            result = con.sql(load_data_sql)
            rows_inserted = con.sql("SELECT COUNT(*) FROM bronze.Dim_Customer").fetchone()[0]
            
            # --- C. Verification ---
            verification_df = con.sql("SELECT * FROM bronze.Dim_Customer LIMIT 5").df()
            
            print(f"\n Data Load Successful! Inserted {rows_inserted} rows.")
            print("\n--- Verified Data Sample ---")
            print(verification_df)
            
        except Exception as e:
            print(f"\nERROR during data loading: {e}")\
            
            if con:
                con.close()
                print("Connection closed")
            

        return  {
                "count": rows_inserted,
                "timestamp": timestamp.isoformat(),
                "status_1":  "Table 'Dim_Customer' ensured to is dropped",
                "status_2":  "Table 'Dim_Customer' ensured to is createted",
                "status_3":  "Table 'Dim_Customer' ensured to is loaded",
            }
    
      




    ############################################
    #We going to add the DimProducts here
    ########################################

    def BronzeDim_Product(self):
        duckdb =duckconnect()
        con=duckdb.duckconnector()
        

        #drop the table
        #create a bronze schema if not exists

        drop_table_sql = """
        CREATE SCHEMA IF NOT EXISTS bronze;
        drop table IF EXISTS  bronze.Dim_Product;
        """
        con.sql(drop_table_sql)
        print("Table 'PromotionType' ensured to is dropped")

        #schema  creation table
        create_table_sql_dim_Product = """
        CREATE TABLE IF NOT EXISTS bronze.Dim_Product (
            ProductID VARCHAR PRIMARY KEY,   -- Primary Key
            ProductName VARCHAR,
            Category VARCHAR,
            Price DOUBLE                     -- Number type in DuckDB is DOUBLE
        );
        """
        con.sql(create_table_sql_dim_Product)
        print("Table 'PromotionType' ensured to exist.")
        latest_file = latest.lastestinstance(Buckname="bronze", sub_dirc="dim_product")


        load_data_sql = f"""
        INSERT INTO bronze.Dim_Product
        SELECT 
            ProductID, 
            ProductName, 
            Category, 
            Price
            FROM read_json_auto('s3://bronze/dim_product/{latest_file}/*.json');
        """

        table_count = con.sql(load_data_sql)
        
        rows_inserted = 0
        try:
            # Execute the load command
            result = con.sql(load_data_sql)
            rows_inserted = con.sql("SELECT COUNT(*) FROM bronze.Dim_Product").fetchone()[0]
            
            # --- C. Verification ---
            verification_df = con.sql("SELECT * FROM bronze.Dim_Product LIMIT 5").df()
            
            print(f"\n Data Load Successful! Inserted {rows_inserted} rows.")
            print("\n--- Verified Data Sample ---")
            print(verification_df)
            
        except Exception as e:
            print(f"\nERROR during data loading: {e}")

            if con:
                con.close()
                print("Connection closed")

        return  {
                "count": rows_inserted,
                "timestamp": datetime.now(),
                "status_1":  "Table 'Dim_Product' ensured to is dropped",
                "status_2":  "Table 'Dim_Product' ensured to is createted",
                "status_3":  "Table 'Dim_Product' ensured to is loaded",
            }
        con.close()


# BronzePromotiontable()
# BronzeDim_Product()
# BronzeCustomertable()


