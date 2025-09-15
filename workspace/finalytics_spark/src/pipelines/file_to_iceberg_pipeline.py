import logging
from pathlib import Path
from typing import Optional

from source_fetchers.raw_file_data_fetcher import RawFileDataFetcher
from destination_ingesters.iceberg_destination_ingester import IcebergDestinationIngester
from object_managers.schema_manager import SchemaManager

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class FileToIcebergPipeline:
    """
    A pipeline executor that fetches data from a raw file and ingests it into an Iceberg table.
    """

    def __init__(
        self,
        connection_config_file_path: Path,
        schema_config_file_path: Path,
        source_file_path: str,
        source_file_delimiter: str,
        spark_app_name: str,
        iceberg_raw_table: str,
    ):
        """
        Initializes the Iceberg Ingestion Pipeline Executor.

        :param connection_config_file_path: Path to the connection configuration file.
        :param schema_config_file_path: Path to the schema configuration file.
        :param source_file_path: Path to the source file.
        :param source_file_delimiter: Delimiter used in the source file.
        :param spark_app_name: Name of the Spark application.
        :param iceberg_raw_table: Name of the Iceberg table for raw data ingestion.
        """
        self.connection_config_file_path = connection_config_file_path
        self.schema_config_file_path = schema_config_file_path
        self.source_file_path = source_file_path
        self.source_file_delimiter = source_file_delimiter
        self.spark_app_name = spark_app_name
        self.iceberg_raw_table = iceberg_raw_table

        # Initialize the SchemaManager to fetch column list for the Iceberg table
        self.schema_manager = SchemaManager(self.schema_config_file_path)
        self.column_list = self.schema_manager.get_column_list("tables", self.iceberg_raw_table)

    def fetch_file_data(self) -> Optional['DataFrame']:
        """
        Fetches raw data from the source file.

        :return: DataFrame containing raw data, or None if no data is available.
        """
        logger.info("Fetching data from the source file...")
        try:
            file_data_fetcher = RawFileDataFetcher(
                self.source_file_path, self.source_file_delimiter
            )
            raw_df = file_data_fetcher.get_raw_file_data()
            if raw_df is None or raw_df.empty:
                logger.warning("No data found in the source file.")
                return None
            logger.info("Data fetched successfully.")
            return raw_df
        except Exception as e:
            logger.error(f"Error fetching data from the source file: {e}", exc_info=True)
            raise

    def ingest_data(self, raw_df: 'DataFrame') -> None:
        """
        Ingests the raw data into the Iceberg table.

        :param raw_df: DataFrame containing raw data.
        """
        if raw_df is None or raw_df.empty:
            logger.warning("No data available for ingestion. Skipping Iceberg ingestion step.")
            return

        logger.info(f"Ingesting data into Iceberg table '{self.iceberg_raw_table}'...")
        try:
            iceberg_ingester = IcebergDestinationIngester(
                self.connection_config_file_path,
                self.schema_config_file_path,
                self.spark_app_name,
            )
            iceberg_ingester.ingest_data_to_destination(raw_df, self.iceberg_raw_table)
            logger.info("Data ingestion to Iceberg completed successfully.")
        except Exception as e:
            logger.error(f"Error ingesting data to Iceberg: {e}", exc_info=True)
            raise

    def execute_pipeline(self) -> None:
        """
        Executes the complete data pipeline:
        - Fetches data from the source file.
        - Ingests the data into the Iceberg table.
        """
        try:
            raw_df = self.fetch_file_data()
            self.ingest_data(raw_df)
        except Exception as e:
            logger.error(f"Pipeline execution failed: {e}", exc_info=True)
            raise