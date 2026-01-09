##In this code we going to create fact table that is in the ERD daigram 
##This will allow us to make aggregated table for bussiness consumption.
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
# -------------------------------------------
    


import pandas as pd
from helper.duckdbconc import duckconnect
import random
from helper.MinoS3_connection import S3Mino_connection
from datetime import datetime
from helper.latestFolder import lastestFolder


latest= lastestFolder()




mino= S3Mino_connection()



duckdb =duckconnect()

# con=duckdb.duckconnector()

number = random.randint(1,15)

latest_file = latest.lastestinstance(Buckname="silver", sub_dirc="dim_promotion")


class GoldDataParquet:

    def GoldSalesparquet(self):
        number = random.randint(1,15)
        con =None
    
        try:
            con = duckdb.duckconnector()
                #schema  creation table
                
            GoldFactParquet= con.sql(f"""
            select 
                SaleID,               
                CustomerID,
                ProductID,
                PromotionID,
                SaleDate,
                QuantitySold,
                UnitPrice,
                TotalAmount,
                Processed_at
            from (
            SELECT
                uuid() AS SaleID,
                c.CustomerID,
                a.ProductID,
                b.PromotionID,
                c.Processed_at AS SaleDate,
                {number} AS QuantitySold,
                a.Price AS UnitPrice,
                a.Price * {number} AS TotalAmount,
                CURRENT_TIMESTAMP AS Processed_at
            FROM silver.dim_product a    
            inner JOIN silver.dim_promotion b 
                ON a.ProductID = b.ProductID 
            inner JOIN silver.dim_customer c 
                ON c.ProductID = a.ProductID
            ) ;
                            

            """)

            df_parquet=GoldFactParquet.df()
            df_parquet["SaleID"] = df_parquet['SaleID'].astype(str)
            
        


            # Save to JSON
            bucket_name="gold"
            output_path = "fact_sales/fact_sales.parquet"
            output_dir = os.path.dirname(output_path)

            os.makedirs(output_dir, exist_ok=True)
            # ------------------------------------
            df_parquet.to_parquet(output_path)

            #---

            print(f"Parquet file saved as: {output_path}")

            mino.mino_connection("fact_sales", output_path,bucket_name)

            


            return  {
                    "count": len(df_parquet),
                    "Output":  f"{output_path}",
            }
        
        except Exception as e:
            print(f"Error in GoldSalesparquet: {e}")
            raise e

        finally:
            # 5. Close Connection safely
            if con:
                con.close()
                print("Connection closed (Parquet Task).")
    

class GoldFactTable:
    def GoldFactSales(self):
        con = None
        target_table = "gold.Fact_sales"

        try:

            con = duckdb.duckconnector()
                
            drop_table_sql = """
                CREATE SCHEMA IF NOT EXISTS gold;
                drop table IF EXISTS  gold.Fact_sales;
                """

            con.sql(drop_table_sql)
            print("Table 'gold.Fact_sales' is dropped")


                #schema  creation table
            create_table_sql = """
            CREATE TABLE IF NOT EXISTS gold.Fact_sales (
                SaleID STRING DEFAULT uuid() PRIMARY KEY,     
                CustomerID VARCHAR,             -- FK
                ProductID VARCHAR,              -- FK
                PromotionID VARCHAR,            -- FK
                SaleDate TIMESTAMP,    ---SaleDate as proccessed_at
                QuantitySold DOUBLE,
                UnitPrice DOUBLE,
                TotalAmount DOUBLE,
                Processed_at TImestamp
                );
                
            """

            con.sql(create_table_sql)


            latest_file = latest.lastestinstance(Buckname="gold", sub_dirc="fact_sales")
            GoldFacttable= con.sql(f"""

            INSERT INTO gold.Fact_sales 
            select 
                SaleID,               
                CustomerID,
                ProductID,
                PromotionID,
                SaleDate,
                QuantitySold,
                UnitPrice,
                TotalAmount,
                Processed_at
            FROM read_parquet('s3://gold/fact_sales/{latest_file}/*.parquet');   ;
                            

            """)

            table_count = con.sql('''select *  from gold.Fact_sales''').df()
            target_table ="gold.Fact_sales "

            print(table_count)
            con.close()


        #needs to put in the gold parquet file

            return   {
                    "count": len(table_count),
                    "timestamp": datetime.now(),
                    "Output":f"Table save as : {target_table}",
                }
        except Exception as e:
                print(f"Error during processing GoldFactSales: {e}")
                raise e          
                        
        finally:
            # 6. Final Cleanup
            # This runs 100% of the time, guaranteeing the file is unlocked.
            if con:
                try:
                    con.close()
                    print("Connection closed (Fact Table Task).")
                except Exception:
                    pass
