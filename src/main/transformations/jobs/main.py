import datetime
import os
import shutil
import sys

from pyspark.sql.functions import lit, concat_ws, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

from resources.dev import config
from src.main.delete.local_file_delete import delete_local_file
from src.main.download.aws_file_download import S3FileDownloader
from src.main.move.move_files import move_s3_to_s3
from src.main.read.aws_read import S3Reader
from src.main.read.database_read import DatabaseReader
from src.main.transformations.jobs.customer_mart_sql_tranform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimension_tables_join import dimensions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.upload.upload_to_s3 import UploadToS3
from src.main.utility.encrypt_decrypt import decrypt
from src.main.utility.logging_config import logger
from src.main.utility.my_sql_session import get_mysql_connection
from src.main.utility.s3_client_object import S3ClientProvider
from src.main.utility.spark_session import spark_session
from src.main.write.parquet_writer import ParquetWriter

############## GET S3 CLient ###################

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# Now s3_client can be used for s3 operations
response = s3_client.list_buckets()
print(response)
logger.info("List of Buckets: %s", response['Buckets'])

# check if local directory has already a file
# if file is there, check if the same file is present in the staging area with status as A.
# If so then don;t delete but try to re-run
# Else throw an error and do not process the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)
    # print(total_csv_files)
    statement = f"""
        select distinct file_name 
        from {config.database}.{config.product_staging_table} 
        where file_name in ({str(total_csv_files)[1:-1]}) and status='A'"""

    logger.info(f"dynamically statement created: {statement}")
    cursor.execute(statement)
    data = cursor.fetchall()
    if data:
        logger.info(f"Your last run was failed, please check following files: {data}")
    else:
        logger.info(f"File present in {config.local_directory}. "
                    f"However, no matching file was found in staging table with status as Active.")
else:
    logger.info("Last run was successful!!!")

# ensure sales_data_upload_S3.py is run or the s3 sales_data folder has few files in it to process
try:
    s3_reader = S3Reader()
    # Bucket name should come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path)
    logger.info("Absolute path on s3 bucket for csv file %s ", s3_absolute_file_path)

    if not s3_absolute_file_path:
        logger.warning(f"No file available at {folder_path} in S3.")
        raise Exception("No data available in S3 to process.")

except Exception as e:
    logger.error("Exited with error code: %s", e)
    raise e

# ['s3://manish-de-spark-project-1/sales_data/sales_data.csv',
# 's3://manish-de-spark-project-1/sales_data/sales_data_2023-12-13.csv']

prefix = f"s3://{config.bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
# logger.info("File path available on s3 under %s bucket and file path is %s", config.bucket_name, file_paths)
logger.info(f"File path available on S3 under {config.bucket_name} bucket and file path is {file_paths}")

try:
    downloader = S3FileDownloader(s3_client, config.bucket_name, config.local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

# get list of all files in local directory
all_files = os.listdir(config.local_directory)
logger.info(f"list of files present in the local directory after download: {all_files}")

# filter files with .csv extension and create absolute path
if all_files:
    csv_files = []
    error_files = []
    for file in all_files:
        if file.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(config.local_directory, file)))
        else:
            error_files.append(os.path.abspath(os.path.join(config.local_directory, file)))

    if not csv_files:
        logger.error(f"No CSV file available in {config.local_directory} folder to process the request")
        raise Exception("No CSV file available to process the request")

else:
    logger.error("There is no file to process.")
    raise Exception("There is no file to process.")

############### csv files #####################
# csv_files = str(csv_files)[1:-1]
logger.info("**********************Listing the files***********************")
logger.info("listing the csv files that needs to be processed: %s", csv_files)

logger.info("******************* Creating Spark Session *******************")
spark = spark_session()
logger.info("******************* Spark Session Created *******************")

# check the required columns in the schema of csv files
# if not required columns keep it in the list or error _files
# else union all the data into the dataframe

logger.info("Checking schema of data loaded in s3")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv") \
        .option("header", "true") \
        .load(data).columns
    logger.info(f"Schema/Columns in {data} are {data_schema}")
    logger.info(f"Mandatory columns in schema is: {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing column(s) is/are: {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing column for the {data}")
        correct_files.append(data)

logger.info(f"******************** List of correct files******************** {correct_files}")
logger.info(f"******************** List of error files******************** {error_files}")
logger.info(f"******************** Moving the error files to error directory if any********************")

# Move the data to error directory on local
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            # Move the error file from local_directory to local_error folder
            shutil.move(file_path, destination_path)
            logger.info(f"Moved {file_name} file from {file_path} path to {destination_path}.")

            # MOve the error folder in S3 sales_data to error folder
            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")

        else:
            logger.error(f"'{file_path}' does not exist.")

else:
    logger.info("There is no error file available in the error folder local path.")

# Additional columns need to be taken care of
# Determine the extra columns

# Before running the process
# stage table needs to be updated with status as Active(A) or Inactive(I)
logger.info(f"******************************* Updating the product_staging_table to indicate process is started "
            f"************************************")
insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H.%M.%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"""
            INSERT INTO {db_name}.{config.product_staging_table} 
            (file_name, file_location, created_date, status) 
            VALUES('{filename}', '{file}', '{formatted_date}', 'A')
            """
        insert_statements.append(statements)

    logger.info(f"Insert statement created for staging table ---- {insert_statements}")
    logger.info("*************************** Connecting to MySql server ***************************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("*************************** MySql connected Successfully ***************************")

    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("****************** There is no file to process *********************")
    raise Exception("******************* No data is available with correct files **********************")

logger.info("********************************** Staging table updated successfully "
            "***************************************")
logger.info("**************************** Fixing extra columns coming from source *****************************")
schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("store_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("sales_date", DateType(), True),
    StructField("sales_person_id", IntegerType(), True),
    StructField("price", FloatType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("total_cost", FloatType(), True),
    StructField("additional_column", StringType(), True)
])

# Connecting with database reader
# database_client = DatabaseReader(config.url, config.properties)
# logger.info("************************** Creating empty dataframe ***************************")
# final_df_to_process = database_client.create_dataframe(spark, "empty_df_create_table")

final_df_to_process = spark.createDataFrame([], schema=schema)
# final_df_to_process.show()


# Creating a new column with concatenated values of extra columns
for data in correct_files:
    logger.info(f"data: {data}")
    data_df = spark.read.format("csv") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present at source. Extra columns: {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column", concat_ws(", ", *extra_columns)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price",
                    "quantity", "total_cost", "additional_column")
        logger.info(f"processed {data} and added additional columns")
    else:
        data_df = data_df.withColumn("additional_column", lit(None)) \
            .select("customer_id", "store_id", "product_name", "sales_date", "sales_person_id", "price",
                    "quantity", "total_cost", "additional_column")
    final_df_to_process = final_df_to_process.union(data_df)

logger.info("****************** Final dataframe from source which will be going for processing *****************")
final_df_to_process.show()

# Enrich the data with all dimension tables
# also create data mart for sales team their incentive, address and all
# another data mart for customer. who bought how much each days of month
# there should be a file for every month. And inside the file, there should be store_id segregation.
# Read the data from Parquet and generate a csv file in which there should be a sales_person_name,
# sales_person_store_id, sales_person_total_billing_done_for_each_month, total_incentive

# Connecting to databaseReader
database_client = DatabaseReader(config.url, config.properties)

# Creating df for all the tables
# customer table
logger.info("**************** Loading customer table into customer_table_df *********************")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)

# product table
logger.info("**************** Loading product table into product_table_df *********************")
product_table_df = database_client.create_dataframe(spark, config.product_table)

# product_staging_table
logger.info("**************** Loading product staging table into product_staging_table_df *********************")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)

# sales_team table
logger.info("**************** Loading sales team table into sales_team_table_df *********************")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)

# store table
logger.info("**************** Loading store table into store_table_df *********************")
store_table_df = database_client.create_dataframe(spark, config.store_table)

s3_customer_store_sales_df_join = dimensions_table_join(final_df_to_process,
                                                        customer_table_df, store_table_df, sales_team_table_df)

# final enriched data
logger.info("*********************** Final enriched data *****************************")
s3_customer_store_sales_df_join.show()

# Write the customer data in customer_data mart in parquet format
# file will be written to local first
# move the RAW data to s3 bucket for reporting tool
# write reporting data into MYSQL table also

logger.info("********************** Write the data into customer data mart ******************************")
final_customer_data_mart_df = s3_customer_store_sales_df_join.select("ct.customer_id", "ct.first_name", "ct.last_name",
                                                                     "ct.address", "ct.pincode",
                                                                     "phone_number", "sales_date", "total_cost")
logger.info("************** Final Data for Customer Data Mart ********************")
final_customer_data_mart_df.show()

parquet_write = ParquetWriter("overwrite", "parquet")
parquet_write.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

logger.info(f"*************** customer data written to local disk at {config.customer_data_mart_local_file} **********")

# Move the customer data to S3 bucket customer_data_mart
logger.info(f"************ Data Movement from local to S3 for customer data mart **************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.customer_data_mart_local_file)
logger.info(f"{message}")

# sales team data mart
logger.info("**************** Write the data into sales team data mart *******************")
final_sales_team_data_mart_df = (s3_customer_store_sales_df_join
                                 .select("store_id", "sales_person_id",
                                         "sales_person_first_name", "sales_person_last_name", "store_manager_name",
                                         "manager_id", "is_manager", "sales_person_address", "sales_person_pincode",
                                         "sales_date", "total_cost",
                                         expr("SUBSTRING(sales_date,1,7) as sales_month")))

logger.info("********************** Final Data for Sales Team Data Mart ************************")
final_sales_team_data_mart_df.show()
parquet_write.dataframe_writer(final_sales_team_data_mart_df, config.sales_team_data_mart_local_file)
logger.info(f"********** sales team data written to local disk at {config.sales_team_data_mart_local_file} **********")

# Move data to s3 bucket sales_data_mart
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory, config.bucket_name, config.sales_team_data_mart_local_file)

logger.info(f"{message}")

# writing the data into partitions
final_sales_team_data_mart_df.write.format("parquet") \
    .option("header", "true") \
    .mode("overwrite") \
    .partitionBy("sales_month", "store_id") \
    .option("path", config.sales_team_data_mart_partitioned_local_file) \
    .save()

# Move data into s3 partition folder
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root, file)
        relative_file_path = os.path.relpath(local_file_path, config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path, config.bucket_name, s3_key)

# Calculation for customer mart
# find out the customer total purchase every month
# write the data into mysql table
logger.info("******************* Calculating customer every month purchased amount **********************")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("******************* Calculation of customer mart done and written into the tables **********************")

# Calculation for sales team mart
# Find out the total sales done by each sales person every month
# GIve the top performer 1% incentive of total sales of the month
# Rest sales person will get nothing
# Write the data into MySQL table

logger.info("Calculating sales monthly billed amount")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info(f"Calculation of sales mart done and written into the table: {config.sales_team_data_mart_table}")

####################### Last Step #####################

# Move the file into S3 processed folder and delete the local files
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info(f"******************* Deleting sales data from {config.local_directory} *******************")
delete_local_file(config.local_directory)
logger.info(f"******************* Deleted sales data from {config.local_directory} *******************")

logger.info("******************* Deleting customer data mart files from locals *******************")
delete_local_file(config.customer_data_mart_local_file)
logger.info("******************* Deleting customer data mart files from locals *******************")

logger.info("******************* Deleting sales team data mart files from locals *******************")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("******************* Deleting sales team data mart files from locals *******************")

logger.info("******************* Deleting sales team data mart partitioned files from locals *******************")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("******************* Deleting sales team data mart partitioned files from locals *******************")

# update the status in the staging table
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"UPDATE {db_name}.{config.product_staging_table} " \
                     f"SET status = 'I', updated_date = '{formatted_date}' " \
                     f"WHERE file_name = '{filename}'"
        update_statements.append(statements)
    logger.info(f"Update statement created for staging table: {update_statements}")
    logger.info("**************** Connecting to MySQL server *******************")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("*********** MySQL connected successfully **************")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("***************** Error in the process while deleting the files *******************")
    sys.exit()

input("Press enter to terminate")
