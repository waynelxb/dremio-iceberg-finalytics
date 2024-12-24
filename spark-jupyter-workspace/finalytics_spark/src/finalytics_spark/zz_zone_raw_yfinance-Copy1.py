from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import yfinance as yf
import yaml
from datetime import date, datetime, timedelta
from registered_tables import RegisteredTables
import pyspark

# Define the custom exception
class MyCustomException(Exception):
    pass

# Main class for ingestion
class RawYFIngestion:
    # Basic attributes of the class
    def __init__(self, spark, equity_type, zone, sink_table, config_file_path):
        self.spark=spark
        self.equity_type = equity_type
        self.zone=zone
        self.sink_table = sink_table
        self.config_file_path = config_file_path
        self.import_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + str(datetime.now().microsecond)[:3]
        rt=RegisteredTables(self.zone, self.sink_table, self.config_file_path)
        self.registered_column_list = rt.get_column_list()
        self.registered_struct_type = rt.get_struct_type()        
        
    # Function to fetch data from Yahoo Finance
    def fetch_yfinance_record(self, multi_param_pairs):
        try:
            symbol, start_date = multi_param_pairs
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
    
#     # Function to process the records (pass through parameters)
#     def process_yfinance_record(self, single_param_pair):
#         # print(f"Processing {single_param_pair}")
#         return self.fetch_yfinance_record(single_param_pair)

#     # Parallel fetch function
#     def parallel_fetch(self, multi_param_pairs):        
      
#         # Create RDD from the input parameter pairs
#         record_rdd = self.spark.sparkContext.parallelize(multi_param_pairs)
        
#         # Use flatMap to return a flattened list of records
#         results_rdd = record_rdd.flatMap(self.process_yfinance_record)
        
#         # Collect the results from the RDD and convert to a list of tuples
#         # results = results_rdd.collect()        
#         df = self.spark.createDataFrame(results_rdd, self.registered_column_list)   
#         df.show()
#         return df
 

    

# # List of stock symbols and start dates
# yf_param_pairs = [
#     ('AAPL', '2024-12-10'),
#     ('MSFT', '2024-12-10'),
#     ('GOOGL', '2024-12-10'),
# ]

# # Instantiate the class
# stock_stage = RawYFIngestion('stock', 'raw', 'raw.stock_eod_yfinance', 'registered_table_schemas.yaml')

# # Fetch data in parallel
# stock_data_rows = stock_stage.parallel_fetch(yf_param_pairs)
