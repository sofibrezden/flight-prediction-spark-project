from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import uuid

def generate_uuid():
    return str(uuid.uuid4())

uuid_udf = udf(generate_uuid, StringType())

def create_spark_session():
    return SparkSession.builder \
        .appName("Airlines Dimensions") \
        .config("spark.driver.memory", "4g") \
        .getOrCreate()
