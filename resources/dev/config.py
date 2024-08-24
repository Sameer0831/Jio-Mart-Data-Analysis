import os

# =====================
# Configuration Values
# =====================

# The `key` is used for AES encryption and decryption. It is set to a 32-character string to ensure compatibility with AES-256, which requires a 32-byte key. This key should be kept secure and should not be shared publicly.
# Example: "SecureEncryptionKeyForAES256!!!!" (32 characters for AES-256)
key = "SecureEncryptionKeyForAES256!!!!"

# The `salt` is used in the key derivation function (PBKDF2) to add randomness to the encryption key. This ensures that the same key and password will not always produce the same encryption result, enhancing security.
# The salt is at least 16 characters long to provide sufficient randomness.
# Example: "UniqueSaltValue!" (16 characters)
salt = "UniqueSaltValue!"

# The `iv` (Initialization Vector) is used to ensure that identical plaintext blocks result in different ciphertexts when
# encrypted. This is particularly important for AES encryption in CBC (Cipher Block Chaining) mode. The IV must be
# exactly 16 characters long, which corresponds to the AES block size.
# Example: "Initialization31" (16 characters)
iv = "Initialization31"

# Note:
# - Make sure to change these values in a production environment and store them securely.
# - The chosen key, salt, and iv values should match the security standards required by your organization.
# - These values are currently set to meaningful strings for readability and should be handled carefully.


#AWS Access And Secret key
aws_access_key = "U35BbjyA7AWxUf5XH3aQU3wPmDn289PmiLg498ZwEtQ="
aws_secret_key = "cKZXL34HWRBNp4QgFANhAcZPkpXXkpi/s4r+E7nDuFSK0oJZi36kdXJL3vmxILyl"

# To manage configurations and requirements specific to AWS s3 environments.
bucket_name = "jiomart-data-analysis-bucket "
s3_customer_datamart_directory = "customer_data_mart"
s3_sales_datamart_directory = "sales_data_mart"
s3_sales_partitioned_datamart_directory="sales_partitioned_data_mart"
s3_source_directory = "sales_data/"
s3_error_directory = "sales_data_error/"
s3_processed_directory = "sales_data_processed/"



# File Download location
# To manage configurations and requirements specific to local development or testing environments. This can be any location, Not mandatory to have them in the project location only
local_directory = "C:\\Users\\samee\\OneDrive\\Desktop\\Jio-DataSets\\file_from_s3\\"
customer_data_mart_local_file = "C:\\Users\\samee\\OneDrive\\Desktop\\Jio-DataSets\\customer_data_mart\\"
sales_team_data_mart_local_file = "C:\\Users\\samee\\OneDrive\\Desktop\\Jio-DataSets\\sales_team_data_mart\\"
sales_team_data_mart_partitioned_local_file = "C:\\Users\\samee\\OneDrive\\Desktop\\Jio-DataSets\\sales_partition_data\\"
error_folder_path_local = "C:\\Users\\samee\\OneDrive\\Desktop\\Jio-DataSets\\error_files\\"
