import logging
from pathlib import Path
from typing import List

from source_fetchers.raw_yahoo_data_fetcher import RawYahooDataFetcher
from ingesters.iceberg_ingester import IcebergIngester
from managers.database_manager import PgDBManager
from managers.iceberg_manager import IcebergManager
from managers.script_generator import SparkSchemaBasedScriptGenerator
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType, TimestampType, LongType


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class YahooPipeline:
    """
    A pipeline executor that fetches data from Yahoo's API, and ingests them into an Iceberg table.
    """

    def __init__(self,
                pgdb_conn_params: dict,
                grouped_symbol_query: str,
                record_type: str,
                spark_app_name: str,
                spark_conn_params: dict,
                iceberg_raw_table_name: str,
                iceberg_raw_table_definition: str,
                pg_stage_table_name: str,
                ):
        """
        Initializes the Iceberg Ingestion Pipeline Executor.

        :param connection_config_file_path: Path to the PostgreSQL connection configuration file.
        :param schema_config_file_path: Path to the schema configuration file.
        :param record_type: Type of record to fetch from Yahoo API.
        :param grouped_symbol_query: SQL query to fetch grouped symbols.
        :param spark_app_name: Name of the Spark application.
        :param iceberg_raw_table: Name of the Iceberg table for raw data ingestion.
        """

        self.pgdb_conn_params=pgdb_conn_params
        self.spark_app_name = spark_app_name 
        self.spark_conn_params=spark_conn_params
        self.iceberg_raw_table_name = iceberg_raw_table_name
        self.iceberg_raw_table_definition = iceberg_raw_table_definition
        self.grouped_symbol_query=grouped_symbol_query
        self.record_type = record_type

        # Initialize the PostgreSQL database manager
        self.pg_db_manager = PgDBManager(self.pgdb_conn_params)
        # self.pg_db_manager = PgDBManager(self.pgdb_conn_uri) 
        self.pg_stage_table_name= pg_stage_table_name
        self.iceberg_manager=IcebergManager(self.spark_app_name, self.spark_conn_params)  

        self.jdbc_url = f"jdbc:postgresql://{self.pgdb_conn_params['host']}:{self.pgdb_conn_params['port']}/{self.pgdb_conn_params['dbname']}"
        self.jdbc_connection_properties = {
            "user": self.pgdb_conn_params["user"],
            "password": self.pgdb_conn_params["password"],
            "driver": "org.postgresql.Driver"
        }
        

    def get_grouped_symbols(self) -> List[str]:
        """
        Fetches grouped symbols from the PostgreSQL database.
        :return: List of grouped symbols.
        """
        logger.info("Fetching Yahoo data from PostgreSQL...")

        try:
            grouped_symbol_list = self.pg_db_manager.get_sql_script_result_list(self.grouped_symbol_query)
            if not grouped_symbol_list:
                logger.warning("No grouped symbols found in PostgreSQL query.")
                return []
            logger.info(f"Fetched {len(grouped_symbol_list)} records from PostgreSQL.")
            return grouped_symbol_list
        except Exception as e:
            logger.error(f"Error fetching grouped symbols from PostgreSQL: {e}", exc_info=True)
            raise

    def fetch_yahoo_raw_data(self):
        """
        Fetches raw Yahoo data from the Yahoo API.
        :param grouped_symbol_list: List of symbols to fetch data for.
        :return: DataFrame containing Yahoo data.
        """
        grouped_symbol_list=self.get_grouped_symbols()
        if not grouped_symbol_list:
            logger.warning("Skipping Yahoo API fetch as no symbols were retrieved.")
            return None

        logger.info("Fetching Yahoo data from Yahoo API...")
        try:
            raw_yahoo_data_fetcher = RawYahooDataFetcher(self.record_type, grouped_symbol_list)
            raw_records = raw_yahoo_data_fetcher.get_raw_yahoo_data()
            return raw_records
        except Exception as e:
            logger.error(f"Error fetching data from Yahoo API: {e}", exc_info=True)
            raise

    def load_raw_data_from_source_into_iceberg(self):
        """
        Ingests the raw Yahoo data into the Iceberg table.
        :param raw_yahoo_df: DataFrame containing Yahoo data.
        """
        ## Append raw records into iceberg raw table
        # fetch raw records
        logger.info(f"Start to load data into Iceberg table '{self.iceberg_raw_table_name}'...")
        try: 
            raw_records = self.fetch_yahoo_raw_data()
            # Create schema based script generator for raw records and table
            spark_script_generator=SparkSchemaBasedScriptGenerator(self.iceberg_raw_table_name, self.iceberg_raw_table_definition)        
            raw_record_schema=spark_script_generator.get_spark_dataframe_schema()
            # Create dataframe for raw records
            df_raw_records=self.iceberg_manager.create_spark_df_with_schema(raw_records, raw_record_schema) 
            # Generate script for creating table 
            create_iceberg_raw_table_script=spark_script_generator.get_create_spark_table_script()  
            # Insert data into iceberg raw table
            self.iceberg_manager.load_data_from_df_into_iceberg(df_raw_records, self.iceberg_raw_table_name, create_iceberg_raw_table_script)        
            logger.info("Data ingestion to Iceberg completed successfully.")
        except Exception as e:
            logger.error(f"Error ingesting data to Iceberg: {e}", exc_info=True)
            raise


    def load_raw_data_from_iceberg_into_pg(self):
        """
        Load data from Iceberg to PostgreSQL.
        """
        try:       
            logger.info(f"Truncating PostgreSQL table: {self.pg_stage_table_name}...")
            pg_truncate_script = f"TRUNCATE TABLE {self.pg_stage_table_name}"
            self.pg_db_manager.execute_sql_script(pg_truncate_script)

            self.iceberg_manager.load_data_from_iceberg_into_pg(
                self.jdbc_url,
                self.jdbc_connection_properties,
                self.iceberg_raw_table_name,           
                self.pg_stage_table_name,                              
            )
            logger.info("Data loaded into PostgreSQL successfully.")
        except Exception as e:
            logger.error(f"Failed to load data into PostgreSQL: {e}", exc_info=True)
            raise  


    def execute_pipeline(self):
        """
        Executes the complete data pipeline:  
        - Fetch grouped symbols from PostgreSQL.  
        - Retrieve Yahoo data from the API.  
        - Ingest retrieved data into the Iceberg table.  
        """
        try:
            # grouped_symbol_list = self.get_grouped_symbols()
            # raw_yahoo_df = self.fetch_yahoo_raw_data(grouped_symbol_list)

            self.load_raw_data_from_source_into_iceberg() 
            self.load_raw_data_from_iceberg_into_pg()
            # self.ingest_data(raw_yahoo_df)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise
