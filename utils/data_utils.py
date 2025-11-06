import os
import shutil


def read_csv_file(spark, file_path):
    return spark.read.csv(file_path, header=True, inferSchema=True)


def read_flights_data(spark):
    df_2018 = read_csv_file(spark, "data/Combined_Flights_2018.csv")
    df_2019 = read_csv_file(spark, "data/Combined_Flights_2019.csv")
    df_2020 = read_csv_file(spark, "data/Combined_Flights_2020.csv")
    df_2021 = read_csv_file(spark, "data/Combined_Flights_2021.csv")
    df_2022 = read_csv_file(spark, "data/Combined_Flights_2022.csv")

    return df_2018.union(df_2019).union(df_2020).union(df_2021).union(df_2022)


def read_airlines_data(spark):
    return read_csv_file(spark, "data/Airlines.csv")


def save_dimension(df, name):
    temp_path = f"temp_{name}"

    df.coalesce(1).write.mode("overwrite").option("header", True).csv(temp_path)

    for file in os.listdir(temp_path):
        if file.startswith('part-') and file.endswith('.csv'):
            shutil.move(os.path.join(temp_path, file), f"output/{name}.csv")
            break

    shutil.rmtree(temp_path)

    print(f"{name}.csv saved. Count: {df.count()}")
