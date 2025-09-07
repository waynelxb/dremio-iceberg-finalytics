import logging
from pathlib import Path
from typing import List

from source_fetchers.raw_tradingview_data_fetcher import RawTradingViewDataFetcher
from destination_ingesters.iceberg_ingester import IcebergIngester
from object_managers.database_manager import PgDBManager2
from object_managers.spark_manager import SparkManager
from object_managers.script_generator import SparkSchemaBasedScriptGenerator
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType, TimestampType, LongType


# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TradingViewLoader:
    """
    A pipeline executor that fetches data from Yahoo's API, and ingests them into an Iceberg table.
    """

    def __init__(self,
                 pgdb_conn_uri,
                 source_url_query,
                 spark_app_name, 
                 spark_conn_params,
                 iceberg_raw_table_name, 
                 iceberg_raw_table_definition
                ):   
        
        self.pgdb_conn_uri=pgdb_conn_uri
        self.spark_app_name = spark_app_name 
        self.spark_conn_params=spark_conn_params
        self.iceberg_raw_table_name = iceberg_raw_table_name
        self.iceberg_raw_table_definition = iceberg_raw_table_definition
        self.source_url_query=source_url_query        
        self.fin_db_manager = PgDBManager2(self.pgdb_conn_uri)   

    def get_tradingview_url_list(self) -> List[str]:
        """
        Fetches TradingView urls from the PostgreSQL database.
        """
        logger.info("Fetching Trading View url from PostgreSQL...")    

        try:
            print(self.source_url_query)
            query_result = self.fin_db_manager.get_sql_script_result_list(self.source_url_query)
            tradingview_url_list = [url[0] for url in query_result]
       
            if not tradingview_url_list:
                logger.warning("No trading view urls found in PostgreSQL query.")
                return []
  
            logger.info(f"Fetched {len(tradingview_url_list)} records from PostgreSQL.")
            return tradingview_url_list
        except Exception as e:
            logger.error(f"Error fetching urls from PostgreSQL: {e}", exc_info=True)
            raise

    def fetch_tradingview_data(self):
        # Script data from each url then put them together, return as a whole
        logger.info("Fetching TradingView data with urls...")
        try:
            tradingview_url_list = self.get_tradingview_url_list()
            # records is a tuple list [(1,2,3),(3,5,6)]
            records=[]
            for url in tradingview_url_list:
                tradingview_data_fetcher = RawTradingViewDataFetcher(url)
                records.extend(tradingview_data_fetcher.scrape_tradingview_data_from_url(url))

            # Some records may not have all the required fields, if so, exclude such records
            qualified_records = []
            failed_records=[]            
            for row in records:
                if len(row) == len(self.iceberg_raw_table_definition['schema']):  # If row is incomplete
                    qualified_records.append(row)
                else:
                    failed_records.append(row)
            if len(failed_records)>0: 
                print("Some TradingView records were failed...", str(failed_records))
                
            return qualified_records
        except Exception as e:
            logger.error(f"Error fetching data from Yahoo API: {e}", exc_info=True)
            raise

    def load_tradingview_data_into_iceberg(self):    
        ## Append raw records into iceberg raw table
        # fetch raw records
        raw_records = self.fetch_tradingview_data()
        # Create Spark session
        spark_manager=SparkManager(self.spark_app_name, self.spark_conn_params)  
        # Create schema based script generator for raw records and table
        spark_script_generator=SparkSchemaBasedScriptGenerator(self.iceberg_raw_table_name, self.iceberg_raw_table_definition)        
        raw_record_schema=spark_script_generator.get_spark_dataframe_schema()
        # Create dataframe for raw records
        df_raw_records=spark_manager.create_spark_df_with_schema(raw_records, raw_record_schema) 

        # Generate script for creating table 
        create_iceberg_raw_table_script=spark_script_generator.get_create_spark_table_script()  
        # Insert data into iceberg raw table
        spark_manager.insert_into_iceberg_table(df_raw_records, self.iceberg_raw_table_name, create_iceberg_raw_table_script)

        

    # def load_data_from_iceberg_to_pg(self, source_iceberg_table, target_pg_table, load_mode):
    #     """
    #     Load data from Iceberg to PostgreSQL.
    #     """
    #     try:
    #         if load_mode=="TruncateLoad":
    #             logger.info(f"Truncating PostgreSQL table: {self.pg_stage_table}...")
    #             pg_truncate_script = f"TRUNCATE TABLE {target_pg_table}"
    #             pg_db_manager.execute_sql_script(pg_truncate_script)

    #             logger.info(f"Loading data from Iceberg to PostgreSQL table: {self.pg_stage_table}...")
    #             self.iceberg_manager.insert_iceberg_data_into_pg(
    #                 self.iceberg_raw_table,           
    #                 self.pg_stage_table,
    #                 self.fin_db_manager.jdbc_url,
    #                 self.fin_db_manager.jdbc_properties,
    #                 "overwrite",
    #             )
    #         logger.info("Data loaded into PostgreSQL successfully.")
    #     except Exception as e:
    #         logger.error(f"Failed to load data into PostgreSQL: {e}", exc_info=True)
    #         raise


    
 
    def run_loader(self):
        """
        Executes the complete data pipeline:  
        - Fetch grouped symbols from PostgreSQL.  
        - Retrieve Yahoo data from the API.  
        - Ingest retrieved data into the Iceberg table.  
        """
        try:
            # tradingview_url_list = self.get_tradingview_url_list()
            tradingview_data = self.load_tradingview_data_into_iceberg()
            # print(tradingview_data)
            # raw_yahoo_df = self.fetch_yahoo_data(grouped_symbol_list)
            # self.ingest_data(raw_yahoo_df)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise
