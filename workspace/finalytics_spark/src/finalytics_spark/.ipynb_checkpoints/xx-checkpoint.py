class MyModel:
    def __init__(self, spark):
        # Step 3: Store the SparkSession instance
        self.spark = spark

    def run(self):
        # Step 4: Use the SparkSession as needed
        data = [("Alice", 29), ("Bob", 34), ("Cathy", 23)]
        df = self.spark.createDataFrame(data, ["Name", "Age"])
        df.show()
        