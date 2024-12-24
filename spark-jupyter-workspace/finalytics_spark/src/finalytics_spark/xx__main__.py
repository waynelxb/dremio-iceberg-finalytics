from pyspark.sql import SparkSession
from xx import MyModel

def main():
    # Step 1: Create the SparkSession in main
    spark = SparkSession.builder \
        .appName("Example Application") \
        .getOrCreate()

    # Step 2: Pass the SparkSession to the model
    model = MyModel(spark)
    model.run()

if __name__ == "__main__":
    main()