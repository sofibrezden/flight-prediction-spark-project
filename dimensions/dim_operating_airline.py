from pyspark.sql.functions import col
from utils.spark_utils import uuid_udf


def create_dim_operating_airline(flights_df, airlines_df):
    operating = flights_df.select(
        col("IATA_Code_Operating_Airline").alias("iata_code")
    ).distinct()

    dim_operating = operating.join(
        airlines_df.select(
            col("Code").alias("iata_code"),
            col("Description").alias("airline_name")
        ),
        on="iata_code",
        how="left"
    )

    dim_operating = dim_operating.withColumn("operating_airline_id", uuid_udf())

    return dim_operating.select("operating_airline_id", "iata_code", "airline_name")
