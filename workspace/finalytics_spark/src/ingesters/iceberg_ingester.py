import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import yaml
import pandas as pd
from pyspark.sql import SparkSession

from managers.iceberg_manager import IcebergManager
from managers.database_manager import PgDBManager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class IcebergIngester:
    def __init__(
        self,
        spark_app_name, 
        spark_conn_params,
        spark_table_schema        
           
    ):
        self.connection_config_file_path = connection_config_file_path
        self.schema_config_file_path = schema_config_file_path
        self.spark_app_name = spark_app_name   

        self.catalog_uri = spark_conn_params['catalog_uri'] 
        self.warehouse = spark_conn_params['warehouse']     # Minio Address to Write to
        self.storage_uri = spark_conn_params['storage_uri']  # Minio IP address from docker inspec
        self.spark_master_uri = spark_conn_params['spark_master_uri']  # Minio IP address from docker inspec        
        
        # Initialize database and Iceberg managers
        self.iceberg_manager = IcebergManager(
            self.spark_app_name,         
            self.catalog_uri,
            self.warehouse,   # Minio Address to Write to
            self.storage_uri,  # Minio IP address from docker inspec
            self.spark_master_uri  # Minio IP address from docker inspec            
        )    
        self.spark_session=self.iceburg_manager.get_spark_session()
        
            
    def _load_source_df_to_iceberg_raw_table(self, source_df, iceberg_raw_table):
        try:
            logger.info(f"Loading data into Iceberg table: {iceberg_raw_table}...")
            self.iceberg_manager.truncate_iceberg_table(iceberg_raw_table)
            self.iceberg_manager.insert_into_iceberg_table(source_df, iceberg_raw_table)
            logger.info(f"{iceberg_raw_table} was loaded successfully.")
        except Exception as e:
            logger.error(f"Failed to load data into {iceberg_raw_table}: {e}", exc_info=True)
            raise
            
    def _merge_iceberg_raw_to_fin(self):
        pass

    
    def ingest_data_to_destination(self, source_df, iceberg_raw_table):
        """
        Main method to fetch and load EOD records.
        """
        try:
            logger.info(f"Loading source data into {iceberg_raw_table}...")
            self._load_source_df_to_iceberg_raw_table(source_df, iceberg_raw_table)
            logger.info(f"Merging {iceberg_raw_table} into fin table...")
            self._merge_iceberg_raw_to_fin()
            logger.info("Records were loaded successfully.")
        except Exception as e:
            logger.error(f"An error occurred during source data loading: {e}", exc_info=True)
            raise



