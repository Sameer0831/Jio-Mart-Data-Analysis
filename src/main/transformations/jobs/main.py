import os

from resources.dev import config
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *
from src.main.utility.sql_session_script import *
from src.main.DataRead.s3_fileRead import *

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

    statement = """
                select distinct file_name from
                jiomart_dataanalysis.product_staging_table
                where file_name in ({str(total_csv_files)[1:-1]}) and status = 'I'
    """
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
    folder_path = config.s3_source_directory  # Source data directory that contains data files.
    s3_absolute_file_path = s3_reader.list_files(s3_client, config.bucket_name, folder_path=folder_path)
    logger.info("Absolute path on s3 bucket for csv file %s ", s3_absolute_file_path)
    if not s3_absolute_file_path:
        logger.info(f"No files available at {folder_path}")
        raise Exception("No Data Available to process")

except Exception as e:
    logger.info("Exited with error:- %s", e)
    raise e
