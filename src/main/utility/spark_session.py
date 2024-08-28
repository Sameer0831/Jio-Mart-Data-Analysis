import findspark
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *

# To use sql inside our spark, we need to pass this config path
def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("Sameer_spark2")\
        .config("spark.driver.extraClassPath", "C:\\my-sql-connector\\mysql-connector-j-9.0.0\\mysql-connector-j-9.0.0.jar") \
        .getOrCreate()
    logger.info("spark session %s",spark)
    return spark