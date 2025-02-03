import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import yaml
import pandas as pd
from pyspark.sql import SparkSession

from ..source_fetchers.raw_yahoo_data_collector import RawYahooDataCollector
from ..object_managers..iceberg_manager import IcebergManager
from ..object_managers.database_manager import PgDBManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class IcebergDestinationIngester:
    def __init__(
        self,
        connection_config_file_path: Path,
        schema_config_file_path: Path,        
        spark_app_name: str,
        source_df,        
        iceberg_raw_table: str,
        pg_stage_table: str,
        script_merge_pg_stage_into_fin: str        
    ):

        self.connection_config_file_path = connection_config_file_path
        self.schema_config_file_path = schema_config_file_path
        self.spark_app_name = spark_app_name        
        self.source_df=source_df   
        self.iceberg_raw_table=iceberg_raw_table
        self.pg_stage_table=pg_stage_table        
        self.script_merge_pg_stage_into_fin=script_merge_pg_stage_into_fin

        # Initialize database and Iceberg managers
        self.pg_db_manager = PgDBManager(self.connection_config_file_path)
        self.iceberg_manager = IcebergManager(
            self.connection_config_file_path,
            self.schema_config_file_path,
            self.spark_app_name,
        )
        

    # def _fetch_raw_yahoo_data(self) -> pd.DataFrame:
    #     """
    #     Fetch Yahoo EOD data from PostgreSQL and Yahoo API.

    #     Returns:
    #         pd.DataFrame: A DataFrame containing the fetched Yahoo EOD data.
    #     """
    #     # query = f"""
    #     #     SELECT group_id, symbol  
    #     #     FROM ufn_etl_get_grouped_{self.equity_type}_symbol({self.symbols_per_group}) 
    #     #     -- WHERE group_id < 2
    #     # """
    #     try:
    #         logger.info("Fetching Yahoo EOD data from PostgreSQL...")
    #         grouped_symbol_list = self.pg_db_manager.get_sql_script_result_list(self.query_grouped_symbol)
    #         logger.info(f"Fetched {len(grouped_symbol_list)} records from PostgreSQL.")

    #         logger.info("Fetching Yahoo EOD data from Yahoo API...")
    #         raw_yahoo_data_colletor=RawYahooDataCollector(self.record_type, grouped_symbol_list)   
            
    #         raw_yahoo_panda_df = raw_yahoo_data_colletor.get_raw_yahoo_data()
    #         print(raw_yahoo_panda_df)
            
    #         raw_yahoo_panda_df["import_time"] = pd.to_datetime(datetime.now()).tz_localize(None)
    #         logger.info(f"Fetched {len(raw_yahoo_panda_df)} records from Yahoo API.")
    #         return raw_yahoo_panda_df
    #     except Exception as e:
    #         logger.error(f"Failed to fetch Yahoo EOD data: {e}", exc_info=True)
    #         raise
            
    def _load_source_data_to_iceberg_raw_table(self):
        """
        Load data into the Iceberg table.

        Args:
            source_df (pd.DataFrame): The DataFrame containing the Yahoo EOD data.
        """
        try:
            logger.info(f"Loading data into Iceberg table: {self.iceberg_raw_table}...")
            self.iceberg_manager.truncate_iceberg_table(self.iceberg_raw_table)
            self.iceberg_manager.insert_into_iceberg_table(self.source_df, self.iceberg_raw_table)
            logger.info("Data loaded into Iceberg successfully.")
        except Exception as e:
            logger.error(f"Failed to load data into Iceberg: {e}", exc_info=True)
            raise
    

    def _load_iceberg_raw_table_to_pg_stage_table(self):
        """
        Load data from Iceberg to PostgreSQL.
        """ 
        try:
            logger.info(f"Truncating PostgreSQL table: {self.pg_stage_table}...")
            pg_truncate_script = f"TRUNCATE TABLE {self.pg_stage_table}"
            self.pg_db_manager.execute_sql_script(pg_truncate_script)

            logger.info(f"Loading data from Iceberg to PostgreSQL table: {self.pg_stage_table}...")
            
            self.iceberg_manager.insert_iceberg_data_into_pg(
                self.iceberg_raw_table,           
                self.pg_stage_table,
                self.pg_db_manager.jdbc_url,
                self.pg_db_manager.jdbc_properties,
                "overwrite",
            )
            logger.info("Data loaded into PostgreSQL successfully.")
        except Exception as e:
            logger.error(f"Failed to load data into PostgreSQL: {e}", exc_info=True)
            raise
            
    def _merge_pg_stage_into_fin(self):
        """
        Merge data in PostgreSQL.
        """
        try:
            logger.info("Merging data in PostgreSQL...")
            self.pg_db_manager.execute_sql_script(self.script_merge_pg_stage_into_fin)
            logger.info("Data merged successfully in PostgreSQL.")
        except Exception as e:
            logger.error(f"Failed to merge data in PostgreSQL: {e}", exc_info=True)
            raise

    def ingest_yahoo_data(self):
        """
        Main method to fetch and load EOD records.
        """
        try:
            logger.info("Starting EOD records loading process...")
            self._load_source_data_to_iceberg_raw_table()
            self._load_iceberg_raw_table_to_pg_stage_table()
            self._merge_pg_stage_into_fin()
            logger.info("EOD records loaded successfully.")
        except Exception as e:
            logger.error(f"An error occurred during EOD records loading: {e}", exc_info=True)
            raise



