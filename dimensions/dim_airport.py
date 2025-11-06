from pyspark.sql.functions import col, lit
from utils.spark_utils import uuid_udf


def create_dim_airport(flights_df):
    origins = flights_df.select(
        col("Origin").alias("airport_code"),
        col("OriginCityName").alias("city_name"),
        col("OriginStateName").alias("state_name")
    ).distinct()

    destinations = flights_df.select(
        col("Dest").alias("airport_code"),
        col("DestCityName").alias("city_name"),
        col("DestStateName").alias("state_name")
    ).distinct()

    airports = origins.union(destinations).distinct()

    airports = airports.withColumn("country", lit("USA"))
    airports = airports.withColumn("airport_id", uuid_udf())

    return airports.select("airport_id", "airport_code", "city_name", "state_name", "country")
