# =====================
# S3 Client Provider
# =====================
# The `boto3` library is the Amazon Web Services (AWS) SDK for Python. It provides an interface to interact with AWS services, including S3.
# In this code, we are using `boto3` to create an S3 client that allows interaction with Amazon S3 buckets.

import boto3


class S3ClientProvider:
    def __init__(self, aws_access_key=None, aws_secret_key=None):
        """
        Initializes the S3ClientProvider with AWS credentials.

        Parameters:
        - aws_access_key (str): The AWS access key ID.
        - aws_secret_key (str): The AWS secret access key.
        """
        self.aws_access_key = aws_access_key
        self.aws_secret_key = aws_secret_key

        # Create a session with the provided AWS credentials
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )

        # Create an S3 client using the session
        self.s3_client = self.session.client('s3')

    def get_client(self):
        """
        Returns the S3 client instance.

        Returns:
        - boto3.client: The S3 client used for interacting with Amazon S3.
        """
        return self.s3_client


# Example usage:
# s3_provider = S3ClientProvider(aws_access_key='your_access_key', aws_secret_key='your_secret_key')
# s3_client = s3_provider.get_client()
# s3_client.list_buckets()  # Example operation to list all S3 buckets
#
# Note:
# - Replace 'your_access_key' and 'your_secret_key' with actual AWS credentials.
# - Ensure AWS credentials are stored securely and not hard-coded in production environments.
