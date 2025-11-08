from pyspark.sql.functions import col, expr, when, monotonically_increasing_id
from pyspark.sql.types import StringType
from utils.spark_utils import generate_uuid

def create_dim_time(flights_df, spark):
    create_uuid_udf = spark.udf.register("create_uuid", generate_uuid, StringType())

    dep_time_df = flights_df.select(
        expr("substring(cast(DepTime as string), 1, 2)").cast("int").alias("hour"),
        expr("substring(cast(DepTime as string), 3, 2)").cast("int").alias("minute")
    )

    arr_time_df = flights_df.select(
        expr("substring(cast(ArrTime as string), 1, 2)").cast("int").alias("hour"),
        expr("substring(cast(ArrTime as string), 3, 2)").cast("int").alias("minute")
    )

    time_df = dep_time_df.union(arr_time_df)

    time_df = time_df.withColumn(
        "period_of_day",
        when((col("hour") >= 5) & (col("hour") < 12), "morning")
        .when((col("hour") >= 12) & (col("hour") < 17), "afternoon")
        .otherwise("evening")
    )

    time_df = time_df.dropDuplicates(["hour", "minute"]).na.drop()

    time_df = time_df.withColumn("time_id", create_uuid_udf())

    time_df = time_df.select("time_id", "hour", "minute", "period_of_day")

    return time_df