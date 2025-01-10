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
from zz_spark import create_spark_session
from zz_schema_manager import SchemaManager
from zz_raw_yahoo import get_raw_yahooquery, get_raw_yfinance
from collections import defaultdict
import warnings
import logging
import pandas as pd



def insert_iceberg_data_into_pg(conn_config_file, iceberg_source_table, pg_database, pg_sink_table, is_pg_truncate_enabled, is_pg_merge_enabled):   
    try:    
        df_source=spark.read.table(iceberg_source_table)          

        pg_db_mgr=PgDBManager(conn_config_file, pg_database)
        pg_url=pg_db_mgr.jdbc_url
        pg_driver=pg_db_mgr.driver

        if is_pg_truncate_enabled == True:
            pg_truncate_script=f"TRUNCATE TABLE {pg_sink_table}"
            pg_db_mgr.execute_sql_script(pg_truncate_script)
        
        # Write DataFrame to PostgreSQL
        df_source.write.jdbc(url=pg_url, table=pg_sink_table, mode="append", properties={"driver": pg_driver}) 

        if is_pg_merge_enabled == True:
            pg_merge_script = "call fin.usp_load_stock_eod();"
            pg_db_mgr.execute_sql_script(pg_merge_script)
            
    except Exception as e:
        print(f"Error loading pg finalytics: {e}") 



def insert_into_iceberg_table(schema_config_file, spark_source_df, iceberg_sink_table):
    try: 
        schema_manager=SchemaManager(schema_config_file)
        schema_struct_type=schema_manager.get_struct_type("tables", iceberg_sink_table)  
        
        create_table_script = schema_manager.get_create_table_query("tables", iceberg_sink_table)
        spark.sql(create_table_script)
     
        spark_source_df.writeTo(iceberg_sink_table).append()
        # source_spark_df.write.mode("overwrite").saveAsTable(iceberg_sink_table) 

        incremental_count=spark_source_df.count()
        total_count=spark.table(iceberg_sink_table).count()

        print(f"{iceberg_sink_table} was loaded with {incremental_count} records, totally {total_count} records.")
        
    except Exception as e:
        print(f"Error loading lceberg raw table: {e}")



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
hist_data_frames=[]
for group, group_symbols in grouped_symbols.items():
    group_id, group_start_date = group
    print(f"Group Date: {group_start_date}, Group Number: {group_id}, Symbols: {group_symbols}")
    # hist_data=get_raw_yfinance(group_symbols, group_start_date, import_time)
    hist_group_data_frame=get_raw_yahooquery(group_symbols, group_start_date, import_time)    
    hist_data_frames.append(hist_group_data_frame)
    time.sleep(1)
combined_hist_data = pd.concat(hist_data_frames, ignore_index=True)

# # Print final combined_data
# print(combined_hist_data)



# Create Spark Session
spark_app_name="raw_yfinance"
spark=create_spark_session(conn_config_file, spark_app_name)
hist_spark_df = spark.createDataFrame(combined_hist_data)    



# Get iceberg table config info
schema_config_file='cfg_schemas.yaml'
iceberg_raw_stock_eod_table='nessie.raw.stock_eod_yahooquery'

# Check if the Iceberg table exists and truncate it if it does
if spark.catalog.tableExists(iceberg_raw_stock_eod_table):
    spark.sql(f"TRUNCATE TABLE {iceberg_raw_stock_eod_table}")
    print(f"Iceberg table {iceberg_raw_stock_eod_table} truncated successfully.")
else:
    print(f"Iceberg table {iceberg_raw_stock_eod_table} does not exist.")

insert_into_iceberg_table(schema_config_file, hist_spark_df, iceberg_raw_stock_eod_table)



pg_table='stage.stock_eod_quote_yahoo'
is_pg_truncate_enabled=True
is_pg_merge_enabled=True
insert_iceberg_data_into_pg(conn_config_file, iceberg_raw_stock_eod_table, pg_db, pg_table, is_pg_truncate_enabled, is_pg_merge_enabled)  












    

