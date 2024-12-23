import yfinance as yf
from datetime import date, datetime, timedelta
# import psycopg2.extras
from joblib import Parallel, delayed
# import psycopg2
import multiprocessing
# from pgcopy import CopyManager
from io import StringIO
import yaml




# with open("config.yaml","r") as file_object:
#     documents=yaml.safe_load_all(file_object)
#     for doc in documents:
#         doc_name = doc['document_name']
#         if doc_name=='yfinance_stock':
#             registered_col_list=doc['column_list']

#         if doc_name=='iceberg_env':
#             CATALOG_URI = doc['catalog_uri'] # Nessie Server URI
#             WAREHOUSE = doc['warehouse']     # Minio Address to Write to
#             STORAGE_URI = doc['storage_uri'] # Minio IP address from docker inspec



class MyCustomException(Exception):
    pass

class YFinanceStageIngestion:
    def __init__(self, equity_type, destination):
        self.equity_type = equity_type
        self.destination = destination
        self.import_time=datetime.now()
        
        # Get yfinance stock data registered column list
        with open("config.yaml","r") as file_object:
            documents=yaml.safe_load_all(file_object)
            for doc in documents:
                doc_name = doc['document_name']
                if doc_name==f"yfinance_{equity_type}":
                    self.registered_col_list=doc['registered_column_list']

    
        # self.import_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S.") + str(datetime.now().microsecond)[:3]
    
    def fetch_yfinance_record(self, args):
        try:
            symbol = args["symbol"]
            start_date = args["start_date"]
            
            quote = yf.Ticker(symbol)
            # start_date = '2024-12-01'
            current_date = date.today()
            hist = quote.history(start=start_date, end=current_date)
            # import_time=datetime.now()
    
            # Reset index to include the Date column in the DataFrame
            hist.reset_index(inplace=True)
            
#             # Standardize the hist column name by lowering the case of the original column name and replacing space with underscore
#             # This standardized column names are reginstered in configuration file
#             standardized_column_list = [x.lower().replace(" ", "_") for x in hist.columns]  
            
#             # Add symbol and import_time in column list
#             extra_field_list = ['symbol', 'import_time']
#             standardized_column_list.extend(extra_field_list)
            
#             # Check whether the standardized column names match the registered ones   
#             set_standardized = set(standardized_column_list)
#             set_registered = set(self.registered_column_list)            
#             if set_standardized!=set_registered:
#                 raise MyCustomException(f"Error: standardized_column_list {str(standardized_column_list)} does not match registered_column_list {str(registered_column_list)}!")

            # Add symbol and import_time in each record
            hist_records_map = hist.itertuples(index=False)            
            record_list = [tuple(row) + (symbol,) + (self.import_time,) for row in hist_records_map]  
            # return standardized_column_list, record_list
            return record_list
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            return []
        
        
    def parallel_fetch(self, param_pairs):
        record_rdd = spark.sparkContext.parallelize(param_pairs)
        results_rdd = record_rdd.flatMap(fetch_data)        
        results = yfinance_data_rdd.flatMap(self.fetch_yfinance_record).collect()
        flattened_results = [item for sublist in results for item in sublist]
        df = spark.createDataFrame(flattened_results)
        df.show(truncate=False)


#             # Determine the number of processes based on CPU cores
#             num_processes = os.cpu_count() or 4
#             print(f"Using {num_processes} processes...")        
#             with Pool(processes=num_processes) as pool:
#                 results = pool.starmap(self.fetch_yfinance_data, param_pairs) 
#             print(results)
#             return results

stock_stage = YFinanceStageIngestion('stock', 'mytable')
        
# List of tickers to fetch
stock_param_pairs = [("AAPL", "2024-12-1"), ("MSFT", "2024-12-5"), ("GOOGL", "2024-12-9")]  # Add more tickers as needed



# Fetch data in parallel
stock_data_rows = stock_stage.parallel_fetch(stock_param_pairs)

# # Convert the data to a Spark DataFrame
# stock_df = spark.createDataFrame(stock_data_rows, schema=schema)

# # Show some rows
# stock_df.show()
