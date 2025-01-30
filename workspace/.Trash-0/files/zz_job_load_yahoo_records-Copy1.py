import yaml
import warnings
import logging
import pandas as pd
from datetime import datetime
from pyspark.sql import SparkSession
from yahoo_records.zz_database_manager import PgDBManager
from yahoo_records.zz_schema_manager import SchemaManager
from yahoo_records.zz_raw_yahoo import get_yahoo_raw_eod_records_in_groups
from yahoo_records.zz_iceberg_manager import IcebergManager
from yahoo_records.zz_iceberg_pg_operator import IcebergPgOperator
from pathlib import Path
from typing import List


class LoadYahooEOD:
    def __init__(self, yahoo_api: str, equity_type: str, connection_config_file_path: Path, schema_config_file_path: Path, spark_app_name: str):
        self.yahoo_api = yahoo_api
        self.equity_type = equity_type
        self.connection_config_file_path = connection_config_file_path
        self.schema_config_file_path = schema_config_file_path
        self.spark_app_name = spark_app_name
        self.fin_db_manager=PgDBManager(self.connection_config_file_path)
        self.logger = self._setup_logger()
        

    def _setup_logger(self) -> logging.Logger:
        """Setup a logger for the job."""
        logger = logging.getLogger(self.__class__.__name__)
        logger.setLevel(logging.INFO)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        return logger

    def _get_grouped_query_result(self, group_size: int) -> List[dict]:
        """Fetch grouped query results from the database."""
        query = f"""
            SELECT group_id, group_start_date, symbol  
            FROM fin.ufn_etl_get_grouped_{self.equity_type}_eod_start_date({group_size}) 
            WHERE group_id < 3
        """
        return self.fin_db_manager.get_sql_script_result_list(query)

    def fetch_and_process_data(self, group_size: int):
        """Main method to fetch and process EOD records."""
        try:
            self.logger.info("Database manager initialized.")

            # Fetch grouped query results
            query_result = self.get_grouped_query_result(group_size)
            if not query_result:
                self.logger.warning("No data found for the query. Exiting.")
                return

            # Fetch raw data from Yahoo API
            hist_data = get_yahoo_raw_eod_records_in_groups(self.yahoo_api, query_result)
            hist_data["import_time"] = pd.to_datetime(datetime.now()).tz_localize(None)
            self.logger.info("Fetched historical data from Yahoo API.")

            # Create Spark Session and Iceberg table
            iceberg_table = f"nessie.raw.{self.equity_type}_eod_yahoo"
            iceberg_manager = IcebergManager(self.connection_config_file_path, self.schema_config_file_path, self.spark_app_name)
            spark_session = iceberg_manager.get_spark_session()

            # Create Spark DataFrame
            hist_df = spark_session.createDataFrame(hist_data)
            iceberg_manager.truncate_iceberg_table(iceberg_table)
            iceberg_manager.insert_into_iceberg_table(hist_df, iceberg_table)
            self.logger.info("Data inserted into Iceberg table.")

            # Load data into PostgreSQL
            pg_table = f"stage.{self.equity_type}_eod_quote_yahoo"
            self.fin_db_manager.execute_sql_script(f"TRUNCATE TABLE {pg_table}")
            iceberg_pg_operator = IcebergPgOperator(spark_session, self.fin_db_manager.jdbc_url, self.fin_db_manager.jdbc_properties)
            iceberg_pg_operator.insert_iceberg_data_into_pg(iceberg_table, pg_table, jdbc_mode="append")
            self.logger.info("Data inserted into PostgreSQL staging table.")

            # Merge data into final table
            self.fin_db_manager.execute_sql_script(f"CALL fin.usp_load_{self.equity_type}_eod();")
            self.logger.info("Data merged into final PostgreSQL table.")

        except Exception as e:
            self.logger.error(f"Error occurred: {e}", exc_info=True)
            raise

    def validate_paths(self):
        """Validate the configuration file paths."""
        if not self.connection_config_file_path.exists():
            raise FileNotFoundError(f"Connection config file not found: {self.connection_config_file_path}")
        if not self.schema_config_file_path.exists():
            raise FileNotFoundError(f"Schema config file not found: {self.schema_config_file_path}")
        self.logger.info("Configuration file paths validated.")


if __name__ == "__main__":
    # Constants
    spark_app_name = "raw_yfinance"
    equity_type = "etf"
    conn_config_file_relative_path = "config/cfg_connections.yaml"
    schema_config_file_relative_path = "config/cfg_schemas.yaml"

    # Resolve file paths
    project_dir_path = Path(__file__).parent.parent
    conn_config_file_path = project_dir_path / conn_config_file_relative_path
    schema_config_file_path = project_dir_path / schema_config_file_relative_path

    # Initialize and run the job
    job = LoadYahooEOD(equity_type, conn_config_file_path, schema_config_file_path, spark_app_name)
    job.validate_paths()
    job.fetch_and_process_data(group_size=100)
