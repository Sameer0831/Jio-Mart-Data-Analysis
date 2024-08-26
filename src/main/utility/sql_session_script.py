# For getting the credentials through config file

import mysql.connector
from resources.dev import config

def get_mysql_connection():
    """
    Establishes a connection to the MySQL database using credentials from config.py.

    Returns:
        mysql.connector.connection.MySQLConnection: A MySQL connection object.
    """
    try:
        # Retrieve database connection details from config.py
        database_name = config.database_name
        user = config.properties["user"]
        password = config.properties["password"]
        host = "localhost"  # Assuming host is always localhost as per config
        url = config.url  # Use the URL directly from config if needed

        # Establishing connection with MySQL database
        connection = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database_name
        )
        return connection

    except mysql.connector.Error as err:
        # Log or print the error for debugging
        print(f"Error: {err}")
        return None


"""
For manual typing of credentials:
=================================

def get_mysql_connection():
    connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="password",
        database="manish"
    )
    return connection
    
"""
