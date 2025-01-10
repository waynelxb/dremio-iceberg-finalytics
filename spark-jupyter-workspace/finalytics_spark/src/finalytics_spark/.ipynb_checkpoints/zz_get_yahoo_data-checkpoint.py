import os
import yaml
import nbimporter
from datetime import datetime, date
import time
import random
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType,  DateType, TimestampType
from pyspark.sql.functions import to_date, to_timestamp
from lab_database_manager import PgDBManager
from lab_spark import create_spark_session
from lab_schema_manager import SchemaManager
# from zz_raw_yahoo import get_raw_yahooquery, get_raw_yfinance
from zz_raw_yahoo import get_raw_yahooquery, get_raw_yfinance
from collections import defaultdict
import warnings
import logging
import pandas as pd
from yahooquery import Ticker



# Get finalytics connetion info
conn_config_file='cfg_connections.yaml'
pg_db="finalytics"
pg_db_mgr=PgDBManager(conn_config_file, pg_db)
import_time = datetime.now()


# Get symbol_start_date_pairs from finalytics
query="SELECT group_id, group_start_date, symbol from fin.vw_etl_stock_eod_start_date_grouped  WHERE group_start_date <'2025-1-9' Limit 30;"
query_result=pg_db_mgr.get_sql_script_result_list(query)

# Initialize a defaultdict to store the symbols for each (group_date, group_id)
grouped_symbols = defaultdict(list)

# Iterate over the data to group symbols by (group_date, group_id)
for group_id, group_start_date, symbol in query_result:
    # Use a tuple of (group_date, group_id) as the key and append the symbol to the list
    grouped_symbols[(group_id, group_start_date)].append(symbol)



warnings.filterwarnings("ignore", category=FutureWarning, module="yahooquery")

pd_hist_data=[]
all_data=[]
for group, group_symbols in grouped_symbols.items():
    group_id, group_start_date = group
    print(f"Group Date: {group_start_date}, Group Number: {group_id}, Symbols: {group_symbols}")
    # hist_data=get_raw_yfinance(group_symbols, group_start_date, import_time)
    hist_group_data=get_raw_yahooquery(group_symbols, group_start_date, import_time)

    all_data.append(hist_group_data)
    
    # pd_hist_group_data = pd.DataFrame(hist_group_data)
    # pd_hist_group_data = pd_hist_group_data.dropna(axis=1, how='all')
    # pd_hist_data.append(pd_hist_group_data)
    # print(hist_group_data)
    # hist_df = spark.createDataFrame(hist_data)    
    # insert_into_iceberg_table(schema_config_file, hist_df, iceberg_raw_stock_eod_table)
    time.sleep(5)

# print(pd_hist_data)

combined_data = pd.concat(all_data, ignore_index=True)

# # Print final combined_data
print(combined_data)
















    

