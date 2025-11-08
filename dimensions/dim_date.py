from pyspark.sql.functions import col, year, month, dayofmonth, quarter, weekofyear, when, dayofweek, to_date
from pyspark.sql.types import StringType
from utils.spark_utils import generate_uuid

def create_dim_date(flights_df, spark):
    create_uuid_udf = spark.udf.register("create_uuid", generate_uuid, StringType())

    date_df = flights_df.select(
        to_date(col("FlightDate")).alias("flight_date")
    ).distinct()

    date_df = date_df.select(
        "flight_date",
        year("flight_date").alias("year"),
        quarter("flight_date").alias("quarter"),
        month("flight_date").alias("month"),
        dayofmonth("flight_date").alias("day_of_month"),
        weekofyear("flight_date").alias("week_of_year")
    )

    date_df = date_df.withColumn(
        "is_weekend",
        when(dayofweek(col("flight_date")).isin(1, 7), True).otherwise(False)
    )

    date_df = date_df.dropDuplicates(["flight_date"]).na.drop()

    date_df = date_df.withColumn("date_id", create_uuid_udf())

    date_df = date_df.select(
        "date_id",
        "flight_date",
        "year",
        "quarter",
        "month",
        "day_of_month",
        "week_of_year",
        "is_weekend"
    )

    return date_df