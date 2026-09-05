import os

from pyspark.sql import SparkSession

# Java se postavlja jer upotrijebljena inačica Sparka podržava
# Javu zaključno s inačicom 17.
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@17"

# Lazy-load, builder pattern, getOrCreate -> singleton
def get_spark_session(app_name="ETL_App"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.jars", "jars/postgresql-42.7.11.jar") \
        .config("spark.sql.session.timeZone", "Europe/Zagreb") \
        .getOrCreate()
