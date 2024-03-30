from dotenv import load_dotenv
import os

load_dotenv()  # take environment variables from .env.

key = os.getenv("key")
iv = os.getenv("iv")
salt = os.getenv("salt")

# iv = os.getenv("iv")
# # Ensure IV is exactly 16 bytes long
# if len(iv) > 16:
#     iv = iv[:16]
#     print(iv)
# elif len(iv) < 16:
#     iv = iv.ljust(16, '\0')  # Pad with null bytes if necessary
#     print(f"less_iv: {iv}")

# Now, when you encode it, it should be 16 bytes long
# iv_bytes = iv.encode('utf-8')

# AWS Access And Secret key
aws_access_key = "5CAGGzQitq6CncKx7/1YdX2gLFltjGyyVGyAbpI8Lhg="
aws_secret_key = "yA9NqqA6SRAzvkN1KVNNBtQi0CkZQzzaSZuPmWd4BWcF5m2SwRXR1o7DbNp7IevU"

# AWS S3 Bucket and directory details
bucket_name = "manish-de-spark-project-1"
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"
s3_sales_partition_directory = "sales_partitioned_data_mart/"

# Database credential
# MySQL database connection properties
database_name = "manish_spark_de"
url = f"jdbc:mysql://localhost:3306/{database_name}"
properties = {
    "user": os.getenv("mysqluser"),
    "password": os.getenv("mysqlpassword"),
    "driver": "com.mysql.cj.jdbc.Driver"
}

# Mysql connections details
host = os.getenv("mysqlhost")
user = os.getenv("mysqluser")
password = "Mg/OFfuodzkkNUhKmWU7Qg=="
database = f"{database_name}"


# MySql Table name
customer_table_name = "customer"
product_staging_table = "product_staging_table"
product_table = "product"
sales_team_table = "sales_team"
store_table = "store"

# MYSQL Data Mart details
customer_data_mart_table = "customers_data_mart"
sales_team_data_mart_table = "sales_team_data_mart"

# Required columns
mandatory_columns = ["customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price", "quantity",
                     "total_cost"]

# Local File location
local_directory = "/home/hdoop/projects/project-1/spark_data/file_from_s3/"
customer_data_mart_local_file = "/home/hdoop/projects/project-1/spark_data/customer_data_mart/"
sales_team_data_mart_local_file = "/home/hdoop/projects/project-1/spark_data/sales_team_data_mart/"
sales_team_data_mart_partitioned_local_file = "/home/hdoop/projects/project-1/spark_data/sales_partition_data/"
error_folder_path_local = "/home/hdoop/projects/project-1/spark_data/error_files/"
