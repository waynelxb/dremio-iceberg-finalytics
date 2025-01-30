import yaml
import warnings
import logging
import pandas as pd
from datetime import datetime, date
import pyspark
from pyspark.sql import SparkSession
from yahoo_records.zz_database_manager import PgDBManager
from yahoo_records.zz_schema_manager import SchemaManager
from yahoo_records.zz_raw_yahoo import get_yahoo_raw_eod_records, get_yahoo_raw_eod_records_in_groups
from yahoo_records.zz_iceberg_manager import IcebergManager
from yahoo_records.zz_iceberg_pg_operator import IcebergPgOperator
from pathlib import Path

class LoadYahooEOD:
    def __init__(self, equity_type, connecton_config_file_path, schema_config_file_path, spark_app_name):
        self.equity_type = equity_type
        self.connecton_config_file_path = connecton_config_file_path
        self.schema_config_file_path=schema_config_file_path
        self.spark_app_name = spark_app_name        

    def get_eod_records(self, group_size):
        pg_db = "finalytics"
        pg_db_mgr = PgDBManager(self.connecton_config_file_path, pg_db)
        pg_jdbc_url = pg_db_mgr.jdbc_url
        pg_jdbc_properties = pg_db_mgr.jdbc_properties
        query=f"SELECT group_id, group_start_date, symbol  FROM fin.ufn_etl_get_grouped_{self.equity_type}_eod_start_date({group_size}) where group_id <3"
        query_result = pg_db_mgr.get_sql_script_result_list(query)

        yahoo_api = "yahooquery"
        hist_data = get_yahoo_raw_eod_records_in_groups(yahoo_api, query_result)
        hist_data["import_time"] = pd.to_datetime(datetime.now()).tz_localize(None)

        # Create Spark Session
        iceberg_raw_eod_table = f"nessie.raw.{equity_type}_eod_yahoo"
        my_iceberg_manager = IcebergManager(self.connecton_config_file_path, self.schema_config_file_path, self.spark_app_name)
        my_spark_session = my_iceberg_manager.get_spark_session()
        
        # my_spark_session.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw;")
        # my_spark_session.sql("CREATE NAMESPACE IF NOT EXISTS nessie.fin;")

        # create spark data frame
        hist_df = my_spark_session.createDataFrame(hist_data)
        my_iceberg_manager.truncate_iceberg_table(iceberg_raw_eod_table)
        my_iceberg_manager.insert_into_iceberg_table(hist_df, iceberg_raw_eod_table)
        
        pg_table = f"stage.{self.equity_type}_eod_quote_yahoo"
        pg_truncate_script = f"TRUNCATE TABLE {pg_table}"
        pg_db_mgr.execute_sql_script(pg_truncate_script)
        
        my_iceberg_pg_operator = IcebergPgOperator(my_spark_session, pg_jdbc_url, pg_jdbc_properties)
        jdbc_mode = "append"
        my_iceberg_pg_operator.insert_iceberg_data_into_pg(iceberg_raw_eod_table, pg_table, jdbc_mode)
        
        pg_merge_script = f"call fin.usp_load_{self.equity_type}_eod();"
        pg_db_mgr.execute_sql_script(pg_merge_script)
        
        # etl_end_time = datetime.now()
        # delta_time = etl_end_time - etl_start_time
        # print(delta_time)





spark_app_name = "raw_yfinance"
equity_type = "etf"
conn_config_file_relative_path = "config/cfg_connections.yaml"
schema_config_file_relative_path = "config/cfg_schemas.yaml"

project_dir_path = Path(__file__).parent.parent
conn_config_file_path = project_dir_path / conn_config_file_relative_path
schema_config_file_path = project_dir_path / schema_config_file_relative_path


myjob=LoadYahooEOD(equity_type, conn_config_file_path, schema_config_file_path, spark_app_name)
myjob.get_eod_records(100)



# equity_type = "etf"
# conn_config_file_relative_path = "config/cfg_connections.yaml"
# schema_config_file_relative_path = "config/cfg_schemas.yaml"

# project_dir_path = Path(__file__).parent.parent
# conn_config_file_path = project_dir_path / conn_config_file_relative_path
# schema_config_file_path = project_dir_path / schema_config_file_relative_path

# etl_start_time = datetime.now()
# # Get finalytics connetion info
# conn_config_file_path = Path(__file__).parent.parent / conn_config_file_relative_path
# pg_db = "finalytics"
# pg_db_mgr = PgDBManager(conn_config_file_path, pg_db)
# pg_jdbc_url = pg_db_mgr.jdbc_url
# pg_jdbc_properties = pg_db_mgr.jdbc_properties



# # Get symbol_start_date_pairs from finalytics
# group_size = 100
# query = f"SELECT group_id, group_start_date, symbol  FROM fin.ufn_etl_get_grouped_{equity_type}_eod_start_date({group_size}) WHERE group_id=1 LIMIT 10"
# query_result = pg_db_mgr.get_sql_script_result_list(query)

# import_time = datetime.now()
# yahoo_api = "yahooquery"
# hist_data = get_yahoo_raw_eod_records_in_groups(yahoo_api, query_result)
# hist_data["import_time"] = pd.to_datetime(import_time).tz_localize(None)

# # Create Spark Session
# spark_app_name = "raw_yfinance"
# iceberg_raw_eod_table = f"nessie.raw.{equity_type}_eod_yahoo"
# my_iceberg_manager = IcebergManager(conn_config_file_path, schema_config_file_path, spark_app_name)
# my_spark_session = my_iceberg_manager.get_spark_session()

# my_spark_session.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw;")
# my_spark_session.sql("CREATE NAMESPACE IF NOT EXISTS nessie.fin;")

# # create spark data frame
# hist_df = my_spark_session.createDataFrame(hist_data)
# my_iceberg_manager.truncate_iceberg_table(iceberg_raw_eod_table)
# my_iceberg_manager.insert_into_iceberg_table(hist_df, iceberg_raw_eod_table)

# pg_table = 'stage.{equity_type}_eod_quote_yahoo'
# pg_truncate_script = f"TRUNCATE TABLE {pg_table}"
# pg_db_mgr.execute_sql_script(pg_truncate_script)

# my_iceberg_pg_operator = IcebergPgOperator(my_spark_session, pg_jdbc_url, pg_jdbc_properties)
# jdbc_mode = "append"
# my_iceberg_pg_operator.insert_iceberg_data_into_pg(iceberg_raw_eod_table, pg_table, jdbc_mode)

# pg_merge_script = "call fin.usp_load_{equity_type}_eod();"
# pg_db_mgr.execute_sql_script(pg_merge_script)

# etl_end_time = datetime.now()
# delta_time = etl_end_time - etl_start_time
# print(delta_time)
