from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# Initialize Spark session
spark = SparkSession.builder.appName("StockData").getOrCreate()

# Define schema
schema = StructType([
    StructField("Ticker", StringType(), True),
    StructField("Company", StringType(), True),
    StructField("Industry", StringType(), True),
    StructField("MarketCap", DoubleType(), True)
])

# Read the text file
def parse_line(line):
    parts = line.strip().split('\t')
    if len(parts) == 4:
        try:
            parts[3] = float(parts[3].replace('M', 'e6').replace('B', 'e9'))
        except ValueError:
            parts[3] = None
        return parts
    return None

input_file = "stocks.txt"  # Update with the correct file path

# Load and parse data
rdd = spark.sparkContext.textFile(input_file).map(parse_line).filter(lambda x: x is not None)

# Create DataFrame
df = spark.createDataFrame(rdd, schema=schema)

df.show()
