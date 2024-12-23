from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import yfinance as yf
from datetime import datetime, date

# Define the custom exception
class MyCustomException(Exception):
    pass

# Main class for ingestion
class YFinanceStageIngestion:
    def __init__(self, equity_type, destination):
        self.equity_type = equity_type
        self.destination = destination
        self.import_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + str(datetime.now().microsecond)[:3]      

    # Function to fetch data from Yahoo Finance
    def fetch_yfinance_record(self, params):
        try:
            symbol, start_date = params
            # Fetch stock data using yfinance
            quote = yf.Ticker(symbol)
            current_date = date.today()
            hist = quote.history(start=start_date, end=current_date)
            
            # Reset index to include Date as a column and format it
            hist.reset_index(inplace=True)
            hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')

            # Add symbol and import_time to each row
            record_list = [
                tuple(row) + (symbol, self.import_time) for row in hist.itertuples(index=False)
            ]
            return record_list
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return []  # Return an empty list on error
    
    # Function to process the records (pass through parameters)
    def process_yfinance_record(self, param):
        print(f"Processing {param}")
        return self.fetch_yfinance_record(param)

    # Parallel fetch function
    def parallel_fetch(self, param_pairs):
        # Create Spark session
        spark = SparkSession.builder.appName("YahooFinanceData").getOrCreate()
        
        # Create RDD from the input parameter pairs
        record_rdd = spark.sparkContext.parallelize(param_pairs)
        
        # Use flatMap to return a flattened list of records
        results_rdd = record_rdd.flatMap(self.process_yfinance_record)
        
        # Collect the results from the RDD
        results = results_rdd.collect()
        return results

# List of stock symbols and start dates
stock_param_pairs = [
    ('AAPL', '2024-12-10'),
    ('MSFT', '2024-12-10'),
    ('GOOGL', '2024-12-10'),
]

# Instantiate the class
stock_stage = YFinanceStageIngestion('stock', 'mytable')

# Fetch data in parallel
stock_data_rows = stock_stage.parallel_fetch(stock_param_pairs)

# You can also load the result into a DataFrame if required
from pyspark.sql import Row
schema = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume', 'Dividends', 'Stock Splits', 'Ticker', 'ImportTime']
rdd_rows = spark.sparkContext.parallelize(stock_data_rows)
df = spark.createDataFrame(rdd_rows, schema)
df.show()