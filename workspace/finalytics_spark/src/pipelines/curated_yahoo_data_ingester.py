import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import yaml
import pandas as pd
from pyspark.sql import SparkSession
from .database_manager import PgDBManager
from .raw_yahoo_data_collector import RawYahooDataCollector
from .iceberg_manager import IcebergManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CuratedYahooDataIngester:
    def __init__(
        self,
        connection_config_file_path: Path,
        schema_config_file_path: Path,
        spark_app_name: str,
        iceberg_raw_table: str,
        pg_stage_table: str,
        script_merge_pg_stage_into_fin: str
        
    ):
        """
        Initialize the LoadYahooEOD class.

        Args:
            connection_config_file_path (Path): Path to the connection configuration file.
            schema_config_file_path (Path): Path to the schema configuration file.
            spark_app_name (str): Name of the Spark application.
        """
        self.connection_config_file_path = connection_config_file_path
        self.schema_config_file_path = schema_config_file_path
        self.spark_app_name = spark_app_name
        self.iceberg_raw_table=iceberg_raw_table
        self.pg_stage_table=pg_stage_table
        self.script_merge_pg_stage_into_fin=script_merge_pg_stage_into_fin
        

        # Initialize database and Iceberg managers
        self.fin_db_manager = PgDBManager(self.connection_config_file_path)
        self.iceberg_manager = IcebergManager(
            self.connection_config_file_path,
            self.schema_config_file_path,
            self.spark_app_name,
        )
            

    def _load_iceberg_raw_table_to_pg_stage_table(self):
        """
        Load data from Iceberg to PostgreSQL.
        """
        # iceberg_raw_table = f"nessie.raw.{self.equity_type}_eod_yahoo"
        # pg_stage_table = f"stage.{self.equity_type}_eod_quote_yahoo"
        try:
            logger.info(f"Truncating PostgreSQL table: {self.pg_stage_table}...")
            pg_truncate_script = f"TRUNCATE TABLE {self.pg_stage_table}"
            self.fin_db_manager.execute_sql_script(pg_truncate_script)

            logger.info(f"Loading data from Iceberg to PostgreSQL table: {self.pg_stage_table}...")
            self.iceberg_manager.insert_iceberg_data_into_pg(
                self.iceberg_raw_table,           
                self.pg_stage_table,
                self.fin_db_manager.jdbc_url,
                self.fin_db_manager.jdbc_properties,
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
        # pg_merge_script = f"call fin.usp_load_{self.equity_type}_eod();"
        try:
            logger.info("Merging data in PostgreSQL...")
            self.fin_db_manager.execute_sql_script(self.script_merge_pg_stage_into_fin)
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
            raw_yahoo_data = self._fetch_raw_yahoo_data()
            self._load_raw_yahoo_data_to_iceberg_raw_table(raw_yahoo_data)
            self._load_iceberg_raw_table_to_pg_stage_table()
            self._merge_pg_stage_into_fin()
            logger.info("EOD records loaded successfully.")
        except Exception as e:
            logger.error(f"An error occurred during EOD records loading: {e}", exc_info=True)
            raise

