# my_functions.py
from pyspark.sql import SparkSession

def process_data(spark: SparkSession, input_path: str, output_path: str):
    """
    Reads data, performs some transformations, and writes it back.
    """
    df = spark.read.csv(input_path, header=True, inferSchema=True)
    # Perform transformations on df
    df.write.mode("overwrite").parquet(output_path)

def another_spark_task(spark: SparkSession, some_param: str):
    """
    Performs another Spark-related task.
    """
    # Use spark object for various operations
    print(f"Executing another Spark task with param: {some_param}")