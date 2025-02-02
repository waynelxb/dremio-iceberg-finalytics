import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import yaml
import pandas as pd
from pyspark.sql import SparkSession
from .zz_database_manager import PgDBManager
from .zz_raw_yahoo import get_yahoo_raw_eod_records_consolidated
from .zz_iceberg_manager import IcebergManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class LoadYahooEOD:
    def __init__(
        self,
        yahoo_api: str,
        equity_type: str,
        symbols_per_group: int,
        connection_config_file_path: Path,
        schema_config_file_path: Path,
        spark_app_name: str,
    ):
        """
        Initialize the LoadYahooEOD class.

        Args:
            yahoo_api (str): The Yahoo API to use for fetching data.
            equity_type (str): The type of equity (e.g., "etf").
            symbols_per_group (int): The number of symbols in each group to fetch.
            connection_config_file_path (Path): Path to the connection configuration file.
            schema_config_file_path (Path): Path to the schema configuration file.
            spark_app_name (str): Name of the Spark application.
        """
        self.yahoo_api = yahoo_api
        self.equity_type = equity_type
        self.symbols_per_group = symbols_per_group
        self.connection_config_file_path = connection_config_file_path
        self.schema_config_file_path = schema_config_file_path
        self.spark_app_name = spark_app_name

        # Initialize database and Iceberg managers
        self.fin_db_manager = PgDBManager(self.connection_config_file_path)
        self.iceberg_manager = IcebergManager(
            self.connection_config_file_path,
            self.schema_config_file_path,
            self.spark_app_name,
        )

    def _fetch_yahoo_data(self) -> pd.DataFrame:
        """
        Fetch Yahoo EOD data from PostgreSQL and Yahoo API.

        Returns:
            pd.DataFrame: A DataFrame containing the fetched Yahoo EOD data.
        """
        query = f"""
            SELECT group_id, group_start_date, symbol  
            FROM fin.ufn_etl_get_grouped_{self.equity_type}_eod_start_date({self.symbols_per_group}) 
            WHERE group_id < 2
        """
        try:
            logger.info("Fetching Yahoo EOD data from PostgreSQL...")
            query_result = self.fin_db_manager.get_sql_script_result_list(query)
            logger.info(f"Fetched {len(query_result)} records from PostgreSQL.")

            logger.info("Fetching Yahoo EOD data from Yahoo API...")
            hist_panda_df = get_yahoo_raw_eod_records_consolidated(self.yahoo_api, query_result)
            hist_panda_df["import_time"] = pd.to_datetime(datetime.now()).tz_localize(None)
            logger.info(f"Fetched {len(hist_panda_df)} records from Yahoo API.")
            return hist_panda_df
        except Exception as e:
            logger.error(f"Failed to fetch Yahoo EOD data: {e}", exc_info=True)
            raise

    def _load_data_to_iceberg_raw(self, hist_panda_df: pd.DataFrame):
        """
        Load data into the Iceberg table.

        Args:
            hist_panda_df (pd.DataFrame): The DataFrame containing the Yahoo EOD data.
        """
        iceberg_raw_eod_table = f"nessie.raw.{self.equity_type}_eod_yahoo"
        try:
            logger.info(f"Loading data into Iceberg table: {iceberg_raw_eod_table}...")
            self.iceberg_manager.truncate_iceberg_table(iceberg_raw_eod_table)
            self.iceberg_manager.insert_into_iceberg_table(hist_panda_df, iceberg_raw_eod_table)
            logger.info("Data loaded into Iceberg successfully.")
        except Exception as e:
            logger.error(f"Failed to load data into Iceberg: {e}", exc_info=True)
            raise

    def _load_iceberg_raw_to_pg_stage(self):
        """
        Load data from Iceberg to PostgreSQL.
        """
        iceberg_source_raw_table = f"nessie.raw.{self.equity_type}_eod_yahoo"
        pg_sink_stage_table = f"stage.{self.equity_type}_eod_quote_yahoo"
        try:
            logger.info(f"Truncating PostgreSQL table: {pg_sink_stage_table}...")
            pg_truncate_script = f"TRUNCATE TABLE {pg_sink_stage_table}"
            self.fin_db_manager.execute_sql_script(pg_truncate_script)

            logger.info(f"Loading data from Iceberg to PostgreSQL table: {pg_sink_stage_table}...")
            self.iceberg_manager.insert_iceberg_data_into_pg(
                iceberg_source_raw_table,
                pg_sink_stage_table,
                self.fin_db_manager.jdbc_url,
                self.fin_db_manager.jdbc_properties,
                "append",
            )
            logger.info("Data loaded into PostgreSQL successfully.")
        except Exception as e:
            logger.error(f"Failed to load data into PostgreSQL: {e}", exc_info=True)
            raise

    def _merge_pg_stage_into_fin(self):
        """
        Merge data in PostgreSQL.
        """
        pg_merge_script = f"call fin.usp_load_{self.equity_type}_eod();"
        try:
            logger.info("Merging data in PostgreSQL...")
            self.fin_db_manager.execute_sql_script(pg_merge_script)
            logger.info("Data merged successfully in PostgreSQL.")
        except Exception as e:
            logger.error(f"Failed to merge data in PostgreSQL: {e}", exc_info=True)
            raise

    def get_eod_records(self):
        """
        Main method to fetch and load EOD records.
        """
        try:
            logger.info("Starting EOD records loading process...")
            hist_panda_df = self._fetch_yahoo_data()
            self._load_data_to_iceberg_raw(hist_panda_df)
            self._load_iceberg_raw_to_pg_stage()
            self._merge_pg_stage_into_fin()
            logger.info("EOD records loaded successfully.")
        except Exception as e:
            logger.error(f"An error occurred during EOD records loading: {e}", exc_info=True)
            raise

