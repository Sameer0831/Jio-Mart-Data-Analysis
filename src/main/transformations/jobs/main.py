from resources.dev import config
from src.main.utility.encrypt_decrypt import *
from src.main.utility.s3_client_object import *
from src.main.utility.logging_config import *

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