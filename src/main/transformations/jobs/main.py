import datetime
import logging
import os
import shutil
import sys

from pyspark.sql.functions import lit, concat_ws, expr
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

from resources.dev import config
from src.main.DataDelete.local_file_delete import delete_local_file
from src.main.DataMoveWithinS3.move_files import *
from src.main.DataRead.DataBaseRead import DatabaseReader
from src.main.DataUpload.upload_to_s3 import UploadToS3
from src.main.DataWrite.DataFrameFormat_writer import *
from src.main.transformations.jobs.customer_mart_sql_transform_write import customer_mart_calculation_table_write
from src.main.transformations.jobs.dimensions_tables_join import dimesions_table_join
from src.main.transformations.jobs.sales_mart_sql_transform_write import sales_mart_calculation_table_write
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.spark_session import spark_session
from src.main.utility.sql_session_script import *
from src.main.DataRead.s3_fileRead import *
from src.main.download.aws_file_downloader import *


# =====================
# AWS Credentials Retrieval
# =====================
# Fetch AWS access key and secret key from the configuration.
# These keys are expected to be encrypted in the config file and need to be decrypted before use.

aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key

# =====================
# S3 Client Initialization
# =====================
# Create an instance of S3ClientProvider with decrypted AWS credentials.
# This S3 client will be used to interact with AWS S3 services.

s3_client_provider = S3ClientProvider(decrypt(aws_access_key), decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

# =====================
# List S3 Buckets
# =====================
# Use the S3 client to list all buckets in the AWS account.
# The response contains details about each bucket.

response = s3_client.list_buckets()

"""
# Uncomment the following line to print the response and view details of S3 buckets.
# print(response)

# The response object contains various details which can be accessed, such as the list of buckets.

"""

# =====================
# Log Bucket Information
# =====================
# Log the list of buckets using the configured logger.
# This will help track and verify the operation performed.

logger.info("List of Buckets in our AWS : %s", response['Buckets'])

"""
We know that: We will read the data file from s3 bucket to our local system
and process it in our local system and then again write it to s3 bucket

For this, what we will do is:
    1. First,if we started a process to get the file from s3 and then the process is failed then we'll check if the file is downloaded locally during the process.
    2. We check if local directory has already a file. If yes, then check if the same file is present in the staging area
    3. If file is present in the staging area as well then dont delete it instead re-run it. If not present then give an error and process the next file.
    Example: If the file is present in the staging area with Active stage then it means that our process is started and we got the file in our staging area still it failed,
     So we'll try to re-run it.
"""
# This csv_files object will get all the required csv files present in our local specified directory using config files
csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".csv")]
# We will create the connection with our mysql
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files =[]
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    formatted_file_names = ", ".join(f"'{file}'" for file in total_csv_files)
    statement = f"""
                select distinct file_name from
                jiomart_dataanalysis.product_staging_table
                where file_name in ({formatted_file_names}) and status = 'A'
    """
    print(total_csv_files)
    logger.info(f"dynamically statement created: {statement} ")
    cursor.execute(statement)
    data = cursor.fetchall()  # fetches all the data from the sql files/tables

    # To verify this, go to specified local directory and create a csv file
    if data:
        logger.info("Your Last run was failed please check")
    else:
        logger.info("No records found")

else:
    logger.info("Last run was successful!!!")

# Reading files from s3
# bucket_name = config.bucket_name.strip()  # If found any error then check and Remove any leading or trailing whitespace

try:
    s3_reader = S3Reader()
    # We are specifying this folder_path, inorder to avoid reading different multiple folders/directories present in our bucket.
    folder_path = config.s3_source_directory  # Source data directory that contains data files.
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
    logger.info("Absolute path on s3 bucket for csv file %s ", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No Data Available to process")

except Exception as e:
    logger.info("Exited with error:- %s", e)
    raise e

# Assigned these values, to use them directly.
bucket_name = config.bucket_name
local_directory = config.local_directory

prefix = f"s3://{bucket_name}/"
file_paths = [url[len(prefix):] for url in s3_absolute_file_path]
# logging.info("File path available on s3 under %s bucket and folder name is %s", bucket_name, file_paths)
logging.info(f"File path available on s3 under {bucket_name} bucket and folder name is {file_paths}")
try:
    downloader = S3FileDownloader(s3_client, bucket_name,local_directory)
    downloader.download_files((file_paths))
except Exception as e:
    logger.error("File download error: %s", e)
    sys.exit()

# Get a list of all files from the local directory
all_files = os.listdir(local_directory)
logger.info(f"List of files present at my local directory after download {all_files}")

# Filter out files with ".csv" in their names and create absolute paths for them.
if all_files:
    csv_files = []
    error_files = []
    for files in all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory, files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory, files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception("No csv data available to process the request")
else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

logger.info("*********** Listing the file ***********")
logger.info("List of csv files that needs to be processed %s", csv_files)
logger.info("************* Creating Spark Session *************")

spark = spark_session()

logger.info("************** Spark Session Created **************")

# SCHEMA VALIDATION
# ======================

# We'll process only those csv files whose schema is correct, we don't waste time to process the unwanted csv files
# Check for the required column in the schema of csv files
# If there is no required column in the schema then place those files in the error_files folder or list them somewhere.
# If there is required column in the schema of the csv files then union all of them to a single dataframe

logger.info("*********** Checking Schema for Data Loaded in S3 ************ ")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
                    .option("header", "true")\
                    .load(data).columns
    logger.info(f"Schema for {data} is {data_schema}")
    logger.info(f"Mandatory columns in schema should be {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"Missing Columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing column for {data}")
        correct_files.append(data)

logger.info(f"************ List of Correct files ********** {correct_files}")
logger.info(f"************ List of Error files ********** {error_files}")
logger.info(f"************ Moving Error data to error directory if any ********** {error_files}")

# Move data to error directory
error_folder_local_path = config.error_folder_path_local
if error_files:
    for file_path in error_files:
        if os.path.exists(file_path):
            file_name = os.path.basename(file_path)
            destination_path = os.path.join(error_folder_local_path, file_name)

            shutil.move(file_path, destination_path)
            logger.info(f"Moved '{file_name}' from s3 file path to '{destination_path}'")

            source_prefix = config.s3_source_directory
            destination_prefix = config.s3_error_directory

            # Processed data should also moved within s3. Since, we've not processed the complete data
            #
            message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix, file_name)
            logger.info(f"{message}")
        else:
            logger.error(f"'{file_path}' does not exist.")
else:
    logger.info("********** There is no error files available in our dataset *********")


# Additional columns needs to be maintained
# Before running the process, stage table needs to be updated with status as Active(A) or InActive(I)
logger.info(f"********** Updating the product_staging_table that: we have started the process")

insert_statements = []
db_name = config.database_name
current_date = datetime.datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"INSERT INTO {db_name}.{config.product_staging_table} " \
                     f"(file_name, file_location, created_date, status)" \
                     f"VALUES ('{filename}', '{filename}', '{formatted_date}', 'A')"
        insert_statements.append(statements)
    logger.info(f"Insert statement created for staging table --- {insert_statements}")
    logger.info("************ Connecting With MySql Server ***********")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("************* My SQL Server connected Successfully! ************")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("************ There are no files to process ************")
    raise Exception("*********** No Data Available with Correct Files **********")

logger.info("************* Staging table Updated Successfully **************")

logger.info("************ Fixing extra column coming from source ************")

schema = StructType([
        StructField("customer_id", IntegerType(), True),
        StructField("store_id", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("sales_date", DateType(), True),
        StructField("sales_person_id", IntegerType(), True),
        StructField("price", FloatType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("total_cost", FloatType(), True),
        StructField("additional_column", StringType(), True),
])

logger.info("************* Creating empty DataFrame ************")
final_df_to_process = spark.createDataFrame([], schema=schema)

"""
If the above syntax doesn't work then try the below approach by establishing connection with database client
Connecting With DatabaseReader

database_client = DatabaseReader(config.url, config.properties)
CREATE SQL TABLE WITH "empty_df_create_table" in our created database

USE jiomart_dataanalysis; # database

CREATE TABLE empty_df_create_table (
    customer_id INT,
    store_id INT,
    product_name VARCHAR(255),
    sales_date DATE,
    sales_person_id INT,
    price FLOAT,
    quantity INT,
    total_cost FLOAT,
    additional_column VARCHAR(255)
);

final_df_to_process = database_client.create_dataframe(spark, "empty_df_create_table")

"""

"""
To understand, how it is working follow these things:
1. Empty all the local directories we specified.
2. Empty the S3 buckets as well. we'll start from scratch.
3. Then create datasets using the provided scripts (src/test/*.py-files)
4. For better understanding only execute extra_column and less_column file's script
5. Push these datasets to s3 bucket using the dataUpload-script (src/test/sales_data_upload-to_s3)
6. run main.py, this file will read data from s3 to our local directory and process it in the local system.
7. It will push the less-column file to error files since it is not having the mandatory columns
8. Then the script will process the extra-column script and adds am additional column to it and push the extra field's data to this additional column.
9. If we dont have any additional columns and only having the mandatory columns then in the created additional-column it'll populate the null values.
"""
final_df_to_process.show()
for data in correct_files:
    data_df = spark.read.format("csv") \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .load(data)
    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"Extra columns present at source is {extra_columns}")
    # The Source file must contain extra columns so that we can get those extra column's values as outputs. otherwise, we will get nulls as output
    if extra_columns:
        data_df = data_df.withColumn("additional_column",
                                     concat_ws(", ", *extra_columns))\
                .select("customer_id", "store_id", "product_name", "sales_date",
                        "sales_person_id", "price", "quantity", "total_cost", "additional_column")
        logger.info(f"processed {data} and added 'additional_column'")
    else:
        data_df = data_df.withColumn("additional_column", lit(None))\
                .select("customer_id", "store_id", "product_name", "sales_date",
                        "sales_person_id", "price", "quantity", "total_cost", "additional_column")
    final_df_to_process = final_df_to_process.union(data_df)
logger.info("******** Final DataFrame from source which will go to Processing is: ************")
final_df_to_process.show(truncate=False)


# Publish the data from all dimension tables.

# Connecting with DataBaseReader
database_client = DatabaseReader(config.url, config.properties)

# Creating dataFrame for all tables
# 1. Customer Table:
logger.info("************** Loading Customer table into customer_table_df *************")
customer_table_df = database_client.create_dataframe(spark, config.customer_table_name)
customer_table_df.show()

# 2. Product Table
logger.info("************** Loading Product table into product_table_df *************")
product_table_df = database_client.create_dataframe(spark, config.product_table)
product_table_df.show()

# 3. Product_Staging Table
logger.info("************** Loading Product_Staging table into product_staging_table_df *************")
product_staging_table_df = database_client.create_dataframe(spark, config.product_staging_table)
product_staging_table_df.show()

# 4. Sales Team Table
logger.info("************** Loading Staging table into sales_team_table_df *************")
sales_team_table_df = database_client.create_dataframe(spark, config.sales_team_table)
sales_team_table_df.show()

# 5. Store Table
logger.info("************** Loading Store table into store_table_df *************")
store_table_df = database_client.create_dataframe(spark, config.store_table)
store_table_df.show()

s3_customer_store_sales_df_join = dimesions_table_join(final_df_to_process,
                                                        customer_table_df,
                                                        store_table_df,
                                                        sales_team_table_df)

# Final Published Data
logger.info("*********** Final Published Data *************")
s3_customer_store_sales_df_join.show()

# Create data-marts for sales team and customers
# 1. Write the customer data into customer data mart in parquet format
# File will be written to local first and then move the raw data to s3 bucket for reporting tool
# Write reporting data into mysql table as well.
logger.info("*********** Write the data into customer Data Mart ************")
final_customer_data_mart_df = s3_customer_store_sales_df_join\
                                    .select("ct.customer_id",
                                            "ct.first_name", "ct.last_name",
                                            "ct.address","ct.pincode",
                                            "phone_number","sales_date","total_cost")
logger.info("******** Final Data for Customer Data Mart is: **********")
final_customer_data_mart_df.show()

parquet_writer = DataFrameWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_customer_data_mart_df, config.customer_data_mart_local_file)

logger.info(f"****** Customer data Written to local disk at {config.customer_data_mart_local_file}")

# Move data on s3 bucket for customer_data_mart
logger.info(f"********** Data Movement from local to s3 for customer data mart *************")
s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_customer_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,
                                   config.bucket_name,
                                   config.customer_data_mart_local_file)
logger.info(f"{message}")

# 2. Write the Sales Team data into sales_team data mart in parquet format
# File will be written to local first and then move the raw data to s3 bucket for reporting tool
# Write reporting data into mysql table as well.
logger.info("*********** Write the data into sales team Data Mart ************")
final_sales_team_data_mart_df = s3_customer_store_sales_df_join\
                                    .select("store_id",
                                            "sales_person_id",
                                            "sales_person_first_name", "sales_person_last_name",
                                            "store_manager_name", "manager_id", "is_manager",
                                            "sales_person_address", "sales_person_pincode",
                                            "sales_date", "total_cost",
                                            expr("SUBSTRING(sales_date,1,7) as sales_month"))

logger.info("*********** Final Data for Sales Team Data Mart is: **********")
final_sales_team_data_mart_df.show()
# parquet_writer = DataFrameWriter("overwrite","parquet")
parquet_writer.dataframe_writer(final_sales_team_data_mart_df,
                                config.sales_team_data_mart_local_file)
logger.info(f"****** Sales Team data Written to local disk at {config.sales_team_data_mart_local_file}")

# Move data on s3 bucket for sales_team_data_mart
logger.info(f"********** Data Movement from local to s3 for sales team data mart *************")
# s3_uploader = UploadToS3(s3_client)
s3_directory = config.s3_sales_datamart_directory
message = s3_uploader.upload_to_s3(s3_directory,
                                   config.bucket_name,
                                   config.sales_team_data_mart_local_file)
logger.info(f"{message}")

# For every month there should be a file and inside that there should be a store_id segregation
# Read the data from parquet and generate a csv file that contains a sales_person_name, sales_person_store_id, sales_person_total_billing_done_for_each_month, total_incentives
#
# Writing data into partitions
final_sales_team_data_mart_df.write.format("parquet")\
                    .option("header","true")\
                    .mode("overwrite")\
                    .partitionBy("sales_month", "store_id")\
                    .option("path", config.sales_team_data_mart_partitioned_local_file)\
                    .save()

# Move data on s3 for partitioned folder
s3_prefix = "sales_partitioned_data_mart"
current_epoch = int(datetime.datetime.now().timestamp()) * 1000
for root, dirs, files in os.walk(config.sales_team_data_mart_partitioned_local_file):
    for file in files:
        print(file)
        local_file_path = os.path.join(root,file)
        relative_file_path = os.path.relpath(local_file_path,
                                config.sales_team_data_mart_partitioned_local_file)
        s3_key = f"{s3_prefix}/{current_epoch}/{relative_file_path}"
        s3_client.upload_file(local_file_path,config.bucket_name, s3_key)

# Calculation for Customer mart - Business related questions

# 1. Find out the customer total purchase every month and write data to mysql table
logger.info("********** Calculating the purchase amount for each customer on a monthly basis. ************")
customer_mart_calculation_table_write(final_customer_data_mart_df)
logger.info("********* Calculation for customer mart is done and written data to table ************")

# Calculation for Customer mart - Business related questions

# 2. Find out the total sales done by each sales_person every month and give the top performer 1% incentive of total sales of that month
# Other Sales Persons will get nothing
# Write the data to mysql table

logger.info(" ********* Calculating the total sales billed amount for each month by sale persons. ***********")
sales_mart_calculation_table_write(final_sales_team_data_mart_df)
logger.info("********** Calculation for Sales mart id done and written data to table ************")

############## LAST STEP ###############
# =======================================
# Move the file on s3 into processed folder and delete the local files
source_prefix = config.s3_source_directory
destination_prefix = config.s3_processed_directory
message = move_s3_to_s3(s3_client, config.bucket_name, source_prefix, destination_prefix)
logger.info(f"{message}")

logger.info("********* Deleting sales data from local **********")
delete_local_file(config.local_directory)
logger.info("********* Deleted sales data from local ***********")

logger.info("********* Deleting customer data mart files from local **********")
delete_local_file(config.customer_data_mart_local_file)
logger.info("********* Deleted customer data mart from local ***********")

logger.info("********* Deleting sales team data mart files from local **********")
delete_local_file(config.sales_team_data_mart_local_file)
logger.info("********* Deleted sales team data mart from local ***********")

logger.info("********* Deleting sales team data mart partitioned files from local **********")
delete_local_file(config.sales_team_data_mart_partitioned_local_file)
logger.info("********* Deleted sales team data mart partitioned files from local ***********")

# Update the status of staging table
# If the Process is started and executed successfully then the status should be changed to 'I' (InActive)
update_statements = []
if correct_files:
    for file in correct_files:
        filename = os.path.basename(file)
        statements = f"UPDATE {db_name}.{config.product_staging_table} " \
                    f"SET status = 'I', updated_date = '{formatted_date}' " \
                    f"WHERE file_name = '{filename}' "
        update_statements.append(statements)
    logger.info(f"Updated statement created for staging table --- {update_statements}")
    logger.info("********** Connecting to MYSQL server *********** ")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("****** My SQL server connected successfully **********")
    for statement in update_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()
else:
    logger.error("***** There are some errors in process in between *******")
    sys.exit()

input("Press enter to terminate > ")


# Every thing will be deleted after the process is completed. Like sales_data/ folder in our s3-bucket
# So first run src/test/sales_data_upload_to_s3.py file.
# Make sure that our local directory contains the required data files. Example Location:
# file_location = "C:\\Users\\samee\\OneDrive\\Desktop\\Jio-DataSets\\spark_data\\sales_data_to_s3"
# if there is no data at the specified file location the run these "src/test/generate_data* " files first
