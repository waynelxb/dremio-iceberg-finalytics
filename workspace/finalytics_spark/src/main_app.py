# main_app.py
from pyspark.sql import SparkSession
from my_functions import process_data, another_spark_task

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("MySparkApp") \
        .master("local[*]") \
        .getOrCreate()

    # Call functions from my_functions.py, passing the SparkSession
    process_data(spark, "data/input.csv", "data/output.parquet")
    another_spark_task(spark, "example_value")

    spark.stop()