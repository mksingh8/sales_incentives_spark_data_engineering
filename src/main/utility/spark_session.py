import findspark

findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from src.main.utility.logging_config import *


def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("manish_spark2") \
        .config("spark.driver.extraClassPath", "/usr/share/java/mysql-connector-java-8.3.0.jar") \
        .getOrCreate()
    logger.info("spark session %s", spark)
    return spark

# .config("spark.driver.extraClassPath", "/home/hdoop/usr/share/java/mysql-connector-java-8.3.0.jar") \
# .config("spark.jars", "/usr/share/java/mysql-connector-java-8.3.0.jar")
