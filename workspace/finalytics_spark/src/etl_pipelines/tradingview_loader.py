import logging
from pathlib import Path
from typing import List

from source_fetchers.raw_tradingview_data_fetcher import RawTradingViewDataFetcher
# from destination_ingesters.iceberg_destination_ingester import IcebergDestinationIngester
from object_managers.database_manager import PgDBManager2

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TradingViewLoader:
    """
    A pipeline executor that fetches data from Yahoo's API, and ingests them into an Iceberg table.
    """

    def __init__(self,
                 db_conn_uri,
                 iceberg_raw_table, 
                 iceberg_raw_table_schema,
                 spark_app_name,                 
                 tradingview_url_query
                ):
        """
        Initializes the Iceberg Ingestion Pipeline Executor.
        :param connection_config_file_path: Path to the PostgreSQL connection configuration file.
        :param schema_config_file_path: Path to the schema configuration file.
        :param spark_app_name: Name of the Spark application.
        :param iceberg_raw_table: Name of the Iceberg table for raw data ingestion.
        """         
        
        self.db_conn_uri=db_conn_uri
        self.spark_app_name = spark_app_name        
        self.iceberg_raw_table = iceberg_raw_table
        self.iceberg_raw_table_schema = iceberg_raw_table_schema
        self.tradingview_url_query=tradingview_url_query        
        # Initialize the PostgreSQL database manager
        self.fin_db_manager = PgDBManager2(self.db_conn_uri)   
        

    def get_tradingview_url_list(self) -> List[str]:
        """
        Fetches TradingView urls from the PostgreSQL database.
        """
        logger.info("Fetching Trading View url from PostgreSQL...")    

        try:
            print(self.tradingview_url_query)
            query_result = self.fin_db_manager.get_sql_script_result_list(self.tradingview_url_query)
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

    def ingest_data_into_iceberg(self, record_schema, records):
        """
        Ingests the raw Yahoo data into the Iceberg table.
        :param raw_yahoo_df: DataFrame containing Yahoo data.
        """

        df = spark.createDataFrame(records, record_schema)
        # df.select("symbol", "Sector","ImportDatetime").show()   
        
        
        if raw_yahoo_df is None or raw_yahoo_df.empty:
            logger.warning("No data available for ingestion. Skipping Iceberg ingestion step.")
            return

        logger.info(f"Ingesting data into Iceberg table '{self.iceberg_raw_table}'...")
        try:
            yahoo_data_iceberg_ingester = IcebergDestinationIngester(
                self.connection_config_file_path,
                self.schema_config_file_path,
                self.spark_app_name
            )
            yahoo_data_iceberg_ingester.ingest_data_to_destination(raw_yahoo_df, self.iceberg_raw_table)
            logger.info("Data ingestion to Iceberg completed successfully.")
        except Exception as e:
            logger.error(f"Error ingesting data to Iceberg: {e}", exc_info=True)
            raise

    def run_loader(self):
        """
        Executes the complete data pipeline:  
        - Fetch grouped symbols from PostgreSQL.  
        - Retrieve Yahoo data from the API.  
        - Ingest retrieved data into the Iceberg table.  
        """
        try:
            # tradingview_url_list = self.get_tradingview_url_list()
            tradingview_data = self.fetch_tradingview_data()
            # print(tradingview_data)
            # raw_yahoo_df = self.fetch_yahoo_data(grouped_symbol_list)
            # self.ingest_data(raw_yahoo_df)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise
