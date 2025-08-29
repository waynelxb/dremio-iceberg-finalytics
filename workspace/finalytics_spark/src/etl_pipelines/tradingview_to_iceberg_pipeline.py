import logging
from pathlib import Path
from typing import List

from source_fetchers.raw_tradingview_data_fetcher import RawTradingViewDataFetcher
from destination_ingesters.iceberg_destination_ingester import IcebergDestinationIngester
from object_managers.database_manager import PgDBManager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class TradingViewToIcebergPipeline:
    """
    A pipeline executor that fetches data from Yahoo's API, and ingests them into an Iceberg table.
    """

    def __init__(self,
                 connection_config_file_path: Path,
                 schema_config_file_path: Path,
                 spark_app_name: str,                 
                 iceberg_raw_table: str,
                 query_tradingview_url: str
                ):
        """
        Initializes the Iceberg Ingestion Pipeline Executor.

        :param connection_config_file_path: Path to the PostgreSQL connection configuration file.
        :param schema_config_file_path: Path to the schema configuration file.
        :param spark_app_name: Name of the Spark application.
        :param iceberg_raw_table: Name of the Iceberg table for raw data ingestion.
        """

        self.connection_config_file_path = connection_config_file_path
        self.schema_config_file_path = schema_config_file_path
        self.spark_app_name = spark_app_name        
        self.iceberg_raw_table = iceberg_raw_table
        self.query_tradingview_url=query_tradingview_url

        # Initialize the PostgreSQL database manager
        self.fin_db_manager = PgDBManager(self.connection_config_file_path)
        

    def get_tradingview_url_list(self) -> List[str]:
        """
        Fetches TradingView urls from the PostgreSQL database.
        """
        logger.info("Fetching Trading View url from PostgreSQL...")    

        try:
            print(self.query_tradingview_url)
            query_result_tuple_list = self.fin_db_manager.get_sql_script_result_list(self.query_tradingview_url)
            tradingview_url_list = [url[0] for url in query_result_tuple_list]
            print(tradingview_url_list)     
            if not tradingview_url_list:
                logger.warning("No trading view urls found in PostgreSQL query.")
                return []
            print(tradingview_url_list)
            logger.info(f"Fetched {len(tradingview_url_list)} records from PostgreSQL.")
            return tradingview_url_list
        except Exception as e:
            logger.error(f"Error fetching urls from PostgreSQL: {e}", exc_info=True)
            raise

    def fetch_tradingview_data(self):
        """
        Fetches raw Yahoo data from the Yahoo API.

        :param grouped_symbol_list: List of symbols to fetch data for.
        :return: DataFrame containing Yahoo data.
        """
        logger.info("Fetching Yahoo data from Yahoo API...")
        try:
            
            tradingview_data_fetcher = RawTradingViewDataFetcher(self.get_tradingview_url_list())
            return tradingview_data_fetcher.concatenate_tradingview_raw_data()
        except Exception as e:
            logger.error(f"Error fetching data from Yahoo API: {e}", exc_info=True)
            raise

    # def ingest_data(self, raw_yahoo_df):
    #     """
    #     Ingests the raw Yahoo data into the Iceberg table.

    #     :param raw_yahoo_df: DataFrame containing Yahoo data.
    #     """
    #     if raw_yahoo_df is None or raw_yahoo_df.empty:
    #         logger.warning("No data available for ingestion. Skipping Iceberg ingestion step.")
    #         return

    #     logger.info(f"Ingesting data into Iceberg table '{self.iceberg_raw_table}'...")
    #     try:
    #         yahoo_data_iceberg_ingester = IcebergDestinationIngester(
    #             self.connection_config_file_path,
    #             self.schema_config_file_path,
    #             self.spark_app_name
    #         )
    #         yahoo_data_iceberg_ingester.ingest_data_to_destination(raw_yahoo_df, self.iceberg_raw_table)
    #         logger.info("Data ingestion to Iceberg completed successfully.")
    #     except Exception as e:
    #         logger.error(f"Error ingesting data to Iceberg: {e}", exc_info=True)
    #         raise

    def execute_pipeline(self):
        """
        Executes the complete data pipeline:  
        - Fetch grouped symbols from PostgreSQL.  
        - Retrieve Yahoo data from the API.  
        - Ingest retrieved data into the Iceberg table.  
        """
        try:
            # tradingview_url_list = self.get_tradingview_url_list()
            tradingview_data = self.fetch_tradingview_data()
            print(tradingview_data)
            # raw_yahoo_df = self.fetch_yahoo_data(grouped_symbol_list)
            # self.ingest_data(raw_yahoo_df)

        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise
