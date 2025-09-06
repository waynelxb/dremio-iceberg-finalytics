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
        logger.info("Fetching TradingView data with urls...")
        try:
            tradingview_url_list = self.get_tradingview_url_list()                
            all_record_tuple_list=[]
            for url in tradingview_url_list:
                tradingview_data_fetcher = RawTradingViewDataFetcher(url)
                all_record_tuple_list.extend(tradingview_data_fetcher.scrape_tradingview_data_from_url(url))
            return all_record_tuple_list
        except Exception as e:
            logger.error(f"Error fetching data from Yahoo API: {e}", exc_info=True)
            raise

    def ingest_tradingview_data_into_iceberg(self):
        records = self.fetch_tradingview_data()
        fixed_records = []
        for row in records:
            if len(row) == 13:  # If row is incomplete
                # # Pad with None to match schema length
                # row = row + (None,) * (13 - len(row))
                fixed_records.append(row)

        spark_manager=SparkManager(self.spark_app_name, self.spark_conn_params)

        
        spark_script_generator=SparkSchemaBasedScriptGenerator(self.iceberg_raw_table_name, self.iceberg_raw_table_definition)        
        create_iceberg_raw_table_script=spark_script_generator.get_create_spark_table_script()        
        print(create_iceberg_raw_table_script)



        

        sp_df=spark_manager.create_spark_df_with_schema_dict(fixed_records, self.iceberg_raw_table_definition)
        sp_df.show()

        spark_manager.insert_into_iceberg_table(sp_df, self.iceberg_raw_table_name, create_iceberg_raw_table_script)

        
        # spark_manager.create_iceberg_table(self.iceberg_raw_table_name, create_iceberg_raw_table_script)
        
        # schema = StructType([
        #     StructField(field["name"], eval(field["type"])(), field["nullable"])
        #     for field in self.iceberg_raw_table_definition["schema"]
        # ])
        # print(records)
        # print(schema)
        # my_spark_session=spark_manager.get_spark_session()
        # source_df=spark_manager.create_spark_df(records, schema)

        
        # df = spark.createDataFrame(records, self.iceberg_raw_table_schema)
        # # df.select("symbol", "Sector","ImportDatetime").show()   
        
        
        # # if raw_yahoo_df is None or raw_yahoo_df.empty:
        # #     logger.warning("No data available for ingestion. Skipping Iceberg ingestion step.")
        # #     return

        # logger.info(f"Ingesting data into Iceberg table '{self.iceberg_raw_table_name}'...")
        # try:
        #     iceberg_ingester = IcebergIngester(
        #               spark_app_name, 
        #               spark_conn_params,
        #               iceberg_table_name,
        #               iceberg_table_schema  
        #     )
        #     yahoo_data_iceberg_ingester.ingest_data_to_destination(raw_yahoo_df, self.iceberg_raw_table_name)
        #     logger.info("Data ingestion to Iceberg completed successfully.")
        # except Exception as e:
        #     logger.error(f"Error ingesting data to Iceberg: {e}", exc_info=True)
        #     raise

    def run_loader(self):
        """
        Executes the complete data pipeline:  
        - Fetch grouped symbols from PostgreSQL.  
        - Retrieve Yahoo data from the API.  
        - Ingest retrieved data into the Iceberg table.  
        """
        try:
            # tradingview_url_list = self.get_tradingview_url_list()
            tradingview_data = self.ingest_tradingview_data_into_iceberg()
            # print(tradingview_data)
            # raw_yahoo_df = self.fetch_yahoo_data(grouped_symbol_list)
            # self.ingest_data(raw_yahoo_df)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise
