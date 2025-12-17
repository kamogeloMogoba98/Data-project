#we must store the bronzetable data in parquet file in the silver sections 
#Before we do that  do data quality check 
#Load it into tables

import sys
import os

# ----------------- ABSOLUTE PATH FIX -----------------
# 1. Get the ABSOLUTE path to the directory containing this script ('DimGenerators')
script_dir = os.path.dirname(os.path.abspath(__file__))

# 2. Get the ABSOLUTE path to the project root ('portfolio_prject')
# We know the project root is always one directory up from the script's directory.
project_root = os.path.abspath(os.path.join(script_dir, os.pardir))

# 3. Add the project root to sys.path
if project_root not in sys.path:
    # insert(0, ...) ensures it's the first place Python checks
    sys.path.insert(0, project_root)
# -----------------------------------------------------

import pandas as pd
from helper.MinoS3_connection import S3Mino_connection
from datetime import datetime, timedelta
from helper.duckdbconc import duckconnect
from helper.latestFolder import lastestFolder


latest= lastestFolder()


# duckdb =duckconnect()

mino= S3Mino_connection()

# con=duckdb.duckconnector()



#  Save to JSON
bucket_name="silver"



class dimDataParquert:

    """This section covers the data quality validations and deduplication applied to the bronze tables prior to
     loading it into the silver parquet layer stored in the MinIO S3 environment
   """
    def DimPromotionTypeParquert(self):
        con = None

        duckdb =duckconnect()
        con=duckdb.duckconnector()
            
        data_Quality_promotiontype= con.sql("""
        select 
        PromotionID,
        ProductID,
        PromotionName,
        DiscountPercent,
        StartDate,
        EndDate,
        PromotionType, 
        current_timestamp as Processed_at from  bronze.PromotionType
        where PromotionID is not null
            and ProductID is not null
            and PromotionID is not null
            and DiscountPercent is not null 
            and StartDate is not null 
            and EndDate is not null
            ---validate date make sure it make sense 
            and StartDate<EndDate
        """).df()

        data_Quality_promotiontype= data_Quality_promotiontype.drop_duplicates()
        print(data_Quality_promotiontype)

        #if path does not exist create one
        output_path = "silver/dim_promotion.parquet"

        output_dir = os.path.dirname(output_path)

        os.makedirs(output_dir, exist_ok=True)

        data_Quality_promotiontype.to_parquet(output_path, engine='pyarrow')

        print(f"Parquet file saved as: {output_path}")

        mino.mino_connection("dim_promotion", output_path,bucket_name)

        if con:
                con.close()
                print("Connection closed")

        return {
                "count": len(data_Quality_promotiontype),
                "timestamp": datetime.now(),
                "Output":"Parquet file saved in: {output_path}",
            }



    def DimCustomerParquert(self):
        con = None

        duckdb =duckconnect()
        con=duckdb.duckconnector()
            
        ###################################
        ##making changes to the Dim customer 
        ##################################

        data_Quality_Dim_Customer= con.sql("""
        select 
        CustomerID,
        FirstName,
        LastName,
        Email,
        PhoneNumber,
        ProductID,
        current_timestamp as Processed_at from  Bronze.Dim_Customer
        where CustomerID is not null
            and FirstName is not null
            and LastName is not null
            and Email is not null 
            and PhoneNumber is not null 
            and ProductID is not null
            ---validate date make sure it make sense 
            
        """).df()

        data_Quality_Dim_Customer= data_Quality_Dim_Customer.drop_duplicates()
        print(data_Quality_Dim_Customer)

        #if path does not exist create one
        output_path = "silver/dim_Customer.parquet"

        output_dir = os.path.dirname(output_path)

        os.makedirs(output_dir, exist_ok=True)

        data_Quality_Dim_Customer.to_parquet(output_path, engine='pyarrow')

        print(f"Parquet file saved as: {output_path}")

        mino.mino_connection("dim_Customer", output_path,bucket_name)


        return   {
                "count": len(data_Quality_Dim_Customer),
                "timestamp": datetime.now(),
                "Output":"JSON file saved as: {output_path}",
            }


    #creating a file for parquet for Dim_product


    def DimProductParquet(self):
        ###################################
        ##making changes to the Dim Product
        ##################################

        duckdb =duckconnect()
        con=duckdb.duckconnector()


        data_Quality_Dim_Product= con.sql("""
        select 
        ProductID,
        ProductName,
        Category,
        Price,
        current_timestamp as Processed_at from  Bronze.Dim_product
        where ProductID is not null
            and ProductName is not null
            and Category is not null 
            and Price is not null 
            ---validate date make sure it make sense 
            
        """).df()

        data_Quality_Dim_Product= data_Quality_Dim_Product.drop_duplicates()#making sure no duplicates slip through
        

        #if path does not exist create one
        output_path = "silver/dim_Product.parquet"

        output_dir = os.path.dirname(output_path)

        os.makedirs(output_dir, exist_ok=True)

        data_Quality_Dim_Product.to_parquet(output_path, engine='pyarrow')

        print(f"Parquet file saved as: {output_path}")

        mino.mino_connection("dim_Product", output_path,bucket_name)

        if con:
                con.close()
                print("Connection closed")

        return   {
                "count": len(data_Quality_Dim_Product),
                "timestamp": datetime.now(),
                "Output":"JSON file saved as: {output_path}",
            }


class Silvertables():
    """"I would like to send the parquest files of the silver that have been checked to silver 
        tables in duckdb"""
        

    #the latest folder 
        
    def dimPromotionSilverTable(self):
        con = None

        duckdb =duckconnect()
        con=duckdb.duckconnector()


            #drop the table
        drop_table_sql = """
        CREATE SCHEMA IF NOT EXISTS silver;
        drop table IF EXISTS  silver.dim_Promotion;
        """
        con.sql(drop_table_sql)
        print("Table 'PromotionType' ensured to is dropped")
        latest_file = latest.lastestinstance(Buckname="silver", sub_dirc="dim_promotion")
      


        #schema  creation table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS silver.dim_Promotion (
            PromotionID VARCHAR PRIMARY KEY,  -- PK
            ProductID VARCHAR,              -- FK
            PromotionName VARCHAR,
            DiscountPercent DOUBLE,         -- Use DOUBLE for floating point numbers
            StartDate TIMESTAMP,            -- Use TIMESTAMP for datetime objects
            EndDate TIMESTAMP,
            PromotionType VARCHAR,
            Proccessed_at TIMESTAMP
        );
        """
        con.sql(create_table_sql)

        #Get the latest directory 
     


        load_data_sql = f"""
            INSERT INTO silver.dim_Promotion

         
                    SELECT PromotionID,
                    ProductID,
                    PromotionName,
                    DiscountPercent,
                    StartDate,
                    EndDate,
                    PromotionType,
                    processed_at asProcessed_at,
                    FROM read_parquet('s3://silver/dim_promotion/{latest_file}/*.parquet');          
            """
        con.sql(load_data_sql)
        table_count = con.sql('''select *  from silver.dim_Promotion ''').df()
       
        target_table = "silver.dim_Promotion"




        return   {
                "count": len(table_count),
                "timestamp": datetime.now(),
                "Output":f"Table save as : {target_table}",
            }





    def dimProductSilverTable(self):
        con = None

        duckdb =duckconnect()
        con=duckdb.duckconnector()


            #drop the table
        drop_table_sql = """
        CREATE SCHEMA IF NOT EXISTS silver;
        drop table IF EXISTS  silver.dim_Product;
        """
        con.sql(drop_table_sql)
        print("Table 'PromotionType' ensured to is dropped")


        #schema  creation table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS silver.dim_Product (
            ProductID VARCHAR PRIMARY KEY,   -- Primary Key
            ProductName VARCHAR,
            Category VARCHAR,
            Price DOUBLE  ,
            Processed_at Timestamp                  
        );
        """
        con.sql(create_table_sql)

        latest_file = latest.lastestinstance(Buckname="silver", sub_dirc="dim_Product")


        load_data_sql_product = f"""
            INSERT INTO silver.dim_Product 

                    SELECT   ProductID ,
                    ProductName,
                    Category,
                    Price,
                    processed_at asProcessed_at
                FROM read_parquet('s3://silver/dim_Product/{latest_file}/*.parquet');---the latest proceesed results
            """
        table_count_1= con.sql(load_data_sql_product)
        table_count = con.sql('''select *  from silver.dim_Product''').df()
      
        target_table = "silver.dim_Product "


        return   {
                "count": len(table_count),
                "timestamp": datetime.now(),
                "Output":f"Table save as : {target_table}",
            }


       




    def dimCustomerSilverTable(self):
        con = None
        duckdb =duckconnect()
        con=duckdb.duckconnector()
            #drop the table
        drop_table_sql = """
        CREATE SCHEMA IF NOT EXISTS silver;
        drop table IF EXISTS  silver.dim_Customer ;
        """
        con.sql(drop_table_sql)
       


        #schema  creation table
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS silver.dim_Customer (
            CustomerID VARCHAR PRIMARY KEY,    -- Primary Key
                    FirstName VARCHAR,
                    LastName VARCHAR,
                    Email VARCHAR,
                    PhoneNumber VARCHAR,
                    ProductID VARCHAR,
                    Processed_at Timestamp  


        );
        """
        con.sql(create_table_sql)
        latest_file = latest.lastestinstance(Buckname="silver", sub_dirc="dim_Customer")
        


        load_data_sql_product = f"""
            INSERT INTO silver.dim_Customer 

                    SELECT  CustomerID ,
                    FirstName,
                    LastName,
                    Email,
                    PhoneNumber,
                    ProductID,
                    processed_at asProcessed_at
                    FROM read_parquet('s3://silver/dim_Customer/{latest_file}/*.parquet')---the latest proceesed results
                      
            """
        table_count_1= con.sql(load_data_sql_product)
        table_count = con.sql('''select *  from silver.dim_Customer''').df()

       
        target_table = "silver.dim_Customer"

        return   {
                "count": len(table_count),
                "timestamp": datetime.now(),
                "Output":f"Table save as : {target_table}",
            }




