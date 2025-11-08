from utils.spark_utils import create_spark_session
from utils.data_utils import read_flights_data, read_airlines_data, save_dimension
from dimensions.dim_marketing_airline import create_dim_marketing_airline
from dimensions.dim_operating_airline import create_dim_operating_airline
from dimensions.dim_airport import create_dim_airport


def main():
    spark = create_spark_session()

    print("Reading flights data")
    flights_df = read_flights_data(spark)

    print("Reading airlines data")
    airlines_df = read_airlines_data(spark)

    print("Creating Dim_Marketing_Airline")
    dim_marketing = create_dim_marketing_airline(flights_df, airlines_df)
    save_dimension(dim_marketing, "Dim_Marketing_Airline")

    print("Creating Dim_Operating_Airline")
    dim_operating = create_dim_operating_airline(flights_df, airlines_df)
    save_dimension(dim_operating, "Dim_Operating_Airline")

    print("Creating Dim_Airport")
    dim_airport = create_dim_airport(flights_df)
    save_dimension(dim_airport, "Dim_Airport")

    spark.stop()


if __name__ == "__main__":
    main()
