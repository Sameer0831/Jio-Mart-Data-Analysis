import datetime
import logging
import os
import shutil
import sys

from pyspark.sql.functions import lit, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType, FloatType

from resources.dev import config
from src.main.DataMoveWithinS3.move_files import *
from src.main.DataRead.DataBaseRead import DatabaseReader
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

