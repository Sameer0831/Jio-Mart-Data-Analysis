"""
encrypt_decrypt.py

This script provides utility functions for securely encrypting and decrypting sensitive data using the AES (Advanced Encryption Standard) encryption algorithm. It utilizes the `Cryptodome` library, a powerful cryptographic library in Python.

Key Components:
1. AES Encryption (CBC Mode): The script uses AES encryption in CBC (Cipher Block Chaining) mode, which requires a key and an initialization vector (IV) to encrypt and decrypt data securely.
2. Key Derivation (PBKDF2): A key derivation function (PBKDF2) is used to derive a strong encryption key from a user-defined passphrase and a salt. This enhances security by ensuring that even if the passphrase is weak, the resulting encryption key is still strong.
3. Padding and Unpadding: Since AES encryption works on fixed-size blocks of data (16 bytes), input data is padded to ensure its length is a multiple of the block size. The script includes helper functions for adding padding before encryption and removing it after decryption.
4. Base64 Encoding: To ensure encrypted data can be easily stored and transmitted, it is encoded in Base64 format. This converts binary data into a text format, which is more manageable in most systems.

Usage:
- Encryption: Use the `encrypt` function to encrypt any plaintext data (e.g., AWS access keys, secret keys) that you need to store or transmit securely. This function returns the encrypted data in Base64 format.
- Decryption: Use the `decrypt` function to decrypt any encrypted data back to its original plaintext form. This is useful when you need to use the sensitive information that has been stored or transmitted securely.

How to Use:
1. Set the `key`, `salt`, and `iv` values in your configuration (`config.py`): These are used to derive the encryption key and initialize the AES cipher.
   - key: The passphrase used for key derivation.
   - salt: A random string used in key derivation to ensure uniqueness.
   - iv: The initialization vector used in AES encryption, must be 16 bytes long.
2. Encrypt AWS Access and Secret Keys:
   - First, call the `encrypt` function with your AWS access key and secret key as arguments.
   - Copy the generated encrypted text (in Base64 format) and replace the plaintext AWS access key and secret key in your `config.py` file with these encrypted values.
3. Decryption Process:
   - When your application needs to use these AWS keys, call the `decrypt` function with the encrypted data from your `config.py` file.
   - The `decrypt` function will return the original plaintext credentials, which can then be used securely within your application.

Security Notes:
- Ensure that `key`, `salt`, and `iv` are kept secure and are not exposed to unauthorized users.
- Never hardcode sensitive values like AWS keys directly in your scripts. Instead, use this script to encrypt and decrypt such values securely.
- Regularly rotate your encryption keys and update the `key`, `salt`, and `iv` accordingly to maintain security.

Example:
To encrypt an AWS access key and secret key, run the script with the keys as inputs to the `encrypt` function, then copy the encrypted outputs to your `config.py` file. Use the `decrypt` function in your application to retrieve the original keys when needed.

Author: Sameer Shaik
Date: 24-08-2024

"""

# Your script continues here...

import base64
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
import os, sys
from resources.dev import config
# from logging_config import logger

try:
    key = config.key
    iv = config.iv
    salt = config.salt

    if not (key and iv and salt):
        raise Exception(F"Error while fetching details for key/iv/salt")
except Exception as e:
    print(f"Error occured. Details : {e}")
    # logger.error("Error occurred. Details: %s", e)
    sys.exit(0)

BS = 16
pad = lambda s: bytes(s + (BS - len(s) % BS) * chr(BS - len(s) % BS), 'utf-8')
unpad = lambda s: s[0:-ord(s[-1:])]

def get_private_key():
    Salt = salt.encode('utf-8')
    kdf = PBKDF2(key, Salt, 64, 1000)
    key32 = kdf[:32]
    return key32


def encrypt(raw):
    raw = pad(raw)
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw))


def decrypt(enc):
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return unpad(cipher.decrypt(base64.b64decode(enc))).decode('utf8')


# Check whether our encrypt, decrypt functions are working correctly/not.
# print(decrypt("U35BbjyA7AWxUf5XH3aQU3wPmDn289PmiLg498ZwEtQ="))
# print(encrypt(("AKIAQXIMQZN4KEXOGVAE")))


# If Facing any errors, then use the below commented script to debug.

"""
import base64
from Cryptodome.Cipher import AES
from Cryptodome.Protocol.KDF import PBKDF2
import os, sys
from resources.dev import config

try:
    key = config.key
    iv = config.iv
    salt = config.salt

    if not (key and iv and salt):
        raise Exception("Error while fetching details for key/iv/salt")
except Exception as e:
    print(f"Error occurred. Details : {e}")
    sys.exit(0)

BS = 16  # Block size for AES
pad = lambda s: s + (BS - len(s) % BS) * chr(BS - len(s) % BS)
unpad = lambda s: s[:-ord(s[-1:])]

def get_private_key():
    Salt = salt.encode('utf-8')
    kdf = PBKDF2(key, Salt, 64, 1000)
    key32 = kdf[:32]
    return key32

def encrypt(raw):
    raw = pad(raw).encode('utf-8')  # Ensure raw is padded and encoded
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    return base64.b64encode(cipher.encrypt(raw)).decode('utf-8')

def decrypt(enc):
    enc = base64.b64decode(enc)  # Decode from base64
    cipher = AES.new(get_private_key(), AES.MODE_CBC, iv.encode('utf-8'))
    decrypted = cipher.decrypt(enc)
    return unpad(decrypted).decode('utf-8')

# Encrypt the AWS keys for storage (example usage)
encrypted_access_key = encrypt("Enter Your AWS ACCESS KEY")
encrypted_secret_key = encrypt("Enter Your AWS SECRET KEY")

print(f"Encrypted AWS Access Key: {encrypted_access_key}")
print(f"Encrypted AWS Secret Key: {encrypted_secret_key}")

# Decrypt the AWS keys for use (example usage)
print("Decrypted AWS Access Key:", decrypt(encrypted_access_key))
print("Decrypted AWS Secret Key:", decrypt(encrypted_secret_key))

Replace the resultant encrypted access key and secret key in dev/config.py


"""