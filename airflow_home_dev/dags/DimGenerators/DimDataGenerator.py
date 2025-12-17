import pandas as pd
import random
import os
import sys

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

from helper.MinoS3_connection import S3Mino_connection
from faker import Faker 
from datetime import datetime, timedelta


##generatng the data and storing it in the brone zone in S3 mino using mino connector and import
fake = Faker()
mino= S3Mino_connection()

class dimGeneratorData:

   def dimCustomerGenerator(self):
        def generate_customers(num_rows=100):
            customers = []

            for i in range(1, num_rows + 1):
                customers.append({
                    "CustomerID": f"CUST{i:04d}",
                    "FirstName": fake.first_name(),
                    "LastName": fake.last_name(),
                    "Email": fake.email(),
                    "PhoneNumber": fake.phone_number(),
                    "ProductID": f"PROD{random.randint(1, 50):03d}",  # Random FK
                    
                    
                })

            return pd.DataFrame(customers)

        df = generate_customers(100)
        print(df.head())


        #the next step is create json files and store them in S3 buckect.

        bucket_name="bronze"
        output_path = "bronze/DimCustomer.json"
        output_dir = os.path.dirname(output_path)

        os.makedirs(output_dir, exist_ok=True)
        # ------------------------------------
        df.to_json(output_path, orient="records", indent=4)

        print(f"JSON file saved as: {output_path}")

        mino.mino_connection("dim_Customers", output_path,bucket_name )
         
        row_inserted = len(df)

        return   {
                "count": row_inserted,
                "timestamp": datetime.now(),
                 "Output_path":"JSON file saved as: {output_path}",
            }
   

   def dimProductGenerator(self):
        def generate_products(num_rows=50):
            product_names = [
                "Laptop", "Smartphone", "Headphones", "Keyboard", "Mouse", "Tablet",
                "Monitor", "Smartwatch", "Printer", "Camera", "Speaker", "Router",
                "Television", "Projector", "Drone", "Hard Drive", "SSD", "USB Stick",
                "Microphone", "Webcam"
            ]

            categories = [
                "Electronics", "Accessories", "Computing", "Audio", "Video", "Gadgets"
            ]

            products = []

            for i in range(1, num_rows + 1):
                products.append({
                    "ProductID": f"PROD{i:03d}",
                    "ProductName": random.choice(product_names),
                    "Category": random.choice(categories),
                    "Price": round(random.uniform(100, 20000), 2)   # random price
                })

            return pd.DataFrame(products)

        # Generate the dataframe
        df_products = generate_products(50)

        # Save to JSON
        bucket_name="bronze"
        output_path = "bronze/Dim_Product.json"
        output_dir = os.path.dirname(output_path)

        os.makedirs(output_dir, exist_ok=True)
        # ------------------------------------
        df_products.to_json(output_path, orient="records", indent=4)

        print(f"JSON file saved as: {output_path}")


        mino.mino_connection("dim_product", output_path, bucket_name)

        row_inserted = len(df_products)
        return   {
                "count": row_inserted,
                "timestamp": datetime.now(),
                "Output_path":"JSON file saved as: {output_path}",
            }
    
   def dimPromotiontGenerator(self):

        def generate_promotions(num_rows=50):
            promotion_names = [
                "Summer Sale", "Winter Clearance", "Flash Deal", "Weekend Special",
                "Holiday Promo", "Buy One Get One", "New Year Discount",
                "Black Friday", "Cyber Monday", "Special Offer"
            ]

            promotion_types = [
                "Seasonal", "Clearance", "Limited Time", "Holiday", "Online", "Store"
            ]

            promotions = []

            for i in range(1, num_rows + 1):
                start_date = fake.date_time_between(start_date="-1m", end_date="now")
                end_date = start_date + timedelta(days=random.randint(3, 30))

                promotions.append({
                    "PromotionID": f"PROM{i:04d}",
                    "ProductID": f"PROD{random.randint(1, 50):03d}",     # Foreign key matching Dim_Product
                    "PromotionName": random.choice(promotion_names),
                    "DiscountPercent": round(random.uniform(5, 70), 2),
                    "StartDate": start_date.isoformat(),
                    "EndDate": end_date.isoformat(),
                    "PromotionType": random.choice(promotion_types)
                })

            return pd.DataFrame(promotions)

        # Generate the dataframe
        df_promotions = generate_promotions(50)
        

        # Save to JSON
        bucket_name="bronze"
        output_path = "bronze/Dim_Promotion.json"
        output_dir = os.path.dirname(output_path)

        os.makedirs(output_dir, exist_ok=True)
        # ------------------------------------
        df_promotions.to_json(output_path, orient="records", indent=4)

        #---

        print(f"JSON file saved as: {output_path}")

        mino.mino_connection("dim_promotion", output_path,bucket_name)

        row_inserted = len(df_promotions)

        return   {
                "count": row_inserted,
                "timestamp": datetime.now(),
                "Output_path":"JSON file saved as: {output_path}",
            }


