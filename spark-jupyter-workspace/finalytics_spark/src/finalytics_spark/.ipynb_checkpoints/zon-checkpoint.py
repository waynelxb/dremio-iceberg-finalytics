from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
import yfinance as yf
import yaml
from datetime import date, datetime, timedelta
from registered_tables import RegisteredTables


# import psycopg2.extras
# import psycopg2
# from pgcopy import CopyManager

# Define the custom exception
class MyCustomException(Exception):
    pass

# Main class for ingestion
class RawYFIngestion:
    def __init__(self, equity_type, zone, sink_table, config_file_path):
        self.equity_type = equity_type
        self.zone=zone
        self.sink_table = sink_table
        self.config_file_path = config_file_path
        self.import_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + str(datetime.now().microsecond)[:3]
        rt=registered_tables(self.zone, self.table, self.config_file_path)
        self.registered_column_list = rt.get_column_list()
        self.registered_struct_type = rt.get_struct_type()
        
    #    # Get registered column list
    #     with open("config.yaml","r") as file_object:
    #         documents=yaml.safe_load_all(file_object)
    #         for doc in documents:
    #             doc_name = doc['document_name']
    #             if doc_name==f"yfinance_{equity_type}":
    #                 self.registered_column_list=doc['registered_column_list']
    # deft get_destination_table_schema(self):
        

#     # Function to fetch data from Yahoo Finance
#     def fetch_yfinance_record(self, multi_param_pairs):
#         try:
#             symbol, start_date = multi_param_pairs
#             # Fetch stock data using yfinance
#             quote = yf.Ticker(symbol)
#             current_date = date.today()
#             hist = quote.history(start=start_date, end=current_date)
            
#             # Reset index to include Date as a column and format it
#             hist.reset_index(inplace=True)
#             hist['Date'] = hist['Date'].dt.strftime('%Y-%m-%d %H:%M:%S')

#             # Add symbol and import_time to each row
#             record_list = [
#                 tuple(row) + (symbol, self.import_time) for row in hist.itertuples(index=False)
#             ]
            
#             return record_list
        
#         except Exception as e:
#             print(f"Error fetching data for {symbol}: {e}")
#             return []  # Return an empty list on error
    
#     # Function to process the records (pass through parameters)
#     def process_yfinance_record(self, single_param_pair):
#         # print(f"Processing {single_param_pair}")
#         return self.fetch_yfinance_record(single_param_pair)

#     # Parallel fetch function
#     def parallel_fetch(self, multi_param_pairs):
        
#         # Create Spark session
#         spark = SparkSession.builder.appName("YahooFinanceData").getOrCreate()
        
#         # Create RDD from the input parameter pairs
#         record_rdd = spark.sparkContext.parallelize(multi_param_pairs)
        
#         # Use flatMap to return a flattened list of records
#         results_rdd = record_rdd.flatMap(self.process_yfinance_record)
        
#         # Collect the results from the RDD and convert to a list of tuples
#         # results = results_rdd.collect()

#         df = spark.createDataFrame(results_rdd, self.registered_column_list)       
#         return df

# List of stock symbols and start dates
yf_param_pairs = [
    ('AAPL', '2024-12-10'),
    ('MSFT', '2024-12-10'),
    ('GOOGL', '2024-12-10'),
]

# Instantiate the class
stock_stage = RawYFIngestion('stock', 'raw', "raw.stock_eod_yfinance", "table_schemas.yaml")

# # Fetch data in parallel
# stock_data_rows = stock_stage.parallel_fetch(yf_param_pairs)

#     def __init__(self, equity_type, zone, sink_table, config_file_path):
#         self.equity_type = equity_type
#         self.zone=zone
#         self.sink_table = sink_table
#         self.config_file_path = config_file_path
#         self.import_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + str(datetime.now().microsecond)[:3]
#         print('zone1: ', self.zone)