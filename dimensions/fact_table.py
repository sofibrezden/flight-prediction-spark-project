from pyspark.sql.functions import col, when, expr
from utils.spark_utils import uuid_udf


def create_fact_flight(flights_df, dim_airport, dim_marketing, dim_operating, dim_date, dim_time):
    fact = flights_df.join(
        dim_airport.select(col("airport_id").alias("origin_airport_id"), col("airport_code")),
        flights_df["Origin"] == col("airport_code"),
        "left"
    ).drop("airport_code")

    fact = fact.join(
        dim_airport.select(col("airport_id").alias("destination_airport_id"), col("airport_code")),
        fact["Dest"] == col("airport_code"),
        "left"
    ).drop("airport_code")

    fact = fact.join(
        dim_marketing.select("marketing_airline_id", "iata_code"),
        fact["IATA_Code_Marketing_Airline"] == col("iata_code"),
        "left"
    ).drop("iata_code")

    fact = fact.join(
        dim_operating.select("operating_airline_id", "iata_code"),
        fact["IATA_Code_Operating_Airline"] == col("iata_code"),
        "left"
    ).drop("iata_code")

    fact = fact.join(
        dim_date.select("date_id", col("flight_date").alias("FlightDate")),
        on="FlightDate",
        how="left"
    )
    dep_time_df = dim_time.select(
        col("time_id").alias("departure_time_id"),
        col("hour").alias("dep_hour_key"),
        col("minute").alias("dep_minute_key")
    )

    fact = fact.withColumn("dep_hour", expr("int(DepTime/100)")) \
        .withColumn("dep_minute", expr("DepTime % 100"))

    fact = fact.join(
        dep_time_df,
        (fact["dep_hour"] == col("dep_hour_key")) &
        (fact["dep_minute"] == col("dep_minute_key")),
        "left"
    ).drop("dep_hour_key", "dep_minute_key")

    fact = fact.withColumn("dep_hour", expr("int(coalesce(DepTime,0)/100)")) \
        .withColumn("dep_minute", expr("coalesce(DepTime,0) % 100"))

    arr_time_df = dim_time.select(
        col("time_id").alias("arrival_time_id"),
        col("hour").alias("arr_hour_key"),
        col("minute").alias("arr_minute_key")
    )

    fact = fact.withColumn("arr_hour", expr("int(ArrTime/100)")) \
        .withColumn("arr_minute", expr("ArrTime % 100"))

    fact = fact.join(
        arr_time_df,
        (fact["arr_hour"] == col("arr_hour_key")) &
        (fact["arr_minute"] == col("arr_minute_key")),
        "left"
    ).drop("arr_hour_key", "arr_minute_key")

    fact = fact.withColumn("arr_hour", expr("int(ArrTime/100)")) \
        .withColumn("arr_minute", expr("ArrTime % 100"))

    fact = fact.withColumn(
        "status",
        when(col("Cancelled") == True, "cancelled")
        .when(col("ArrDelayMinutes") > 30, "delayed")
        .otherwise("on_time")
    )

    fact = fact.withColumn(
        "flight_complexity_score",
        (col("Distance") * 1.60934 / 1000) + (col("DepDelayMinutes") / 30)
    )

    fact = fact.withColumn("flight_id", uuid_udf())
    fact = fact.withColumnRenamed("Tail_Number", "tail_number")

    fact = fact.select(
        "flight_id",
        "date_id",
        "departure_time_id",
        "arrival_time_id",
        "origin_airport_id",
        "destination_airport_id",
        "marketing_airline_id",
        "operating_airline_id",
        "tail_number",
        col("DepDelayMinutes").alias("dep_delay_minutes"),
        col("ArrDelayMinutes").alias("arr_delay_minutes"),
        col("CRSElapsedTime").alias("crs_elapsed_time"),
        col("ActualElapsedTime").alias("actual_elapsed_time"),
        col("Distance").alias("distance"),
        "status",
        "flight_complexity_score"
    )

    return fact