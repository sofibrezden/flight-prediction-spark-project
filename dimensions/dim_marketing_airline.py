from pyspark.sql.functions import col
from utils.spark_utils import uuid_udf


def create_dim_marketing_airline(flights_df, airlines_df):
    marketing = flights_df.select(
        col("IATA_Code_Marketing_Airline").alias("iata_code")
    ).distinct()

    dim_marketing = marketing.join(
        airlines_df.select(
            col("Code").alias("iata_code"),
            col("Description").alias("airline_name")
        ),
        on="iata_code",
        how="left"
    )

    dim_marketing = dim_marketing.withColumn("marketing_airline_id", uuid_udf())

    return dim_marketing.select("marketing_airline_id", "iata_code", "airline_name")
