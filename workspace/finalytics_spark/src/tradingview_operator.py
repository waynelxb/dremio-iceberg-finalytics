import logging
import argparse
from pathlib import Path
from typing import Dict, Any
import yaml
import os
from functools import reduce
import operator

from etl_pipelines.tradingview_loader import TradingViewLoader

# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

def get_nested_dict(dictionary, key_path):
    try:
        return reduce(operator.getitem, key_path, dictionary)
    except (KeyError, TypeError) as e:
        raise KeyError(f"Could not access key path {key_path}: {e}")
      

def main(loader_config_file: str):
    """
    Main function that loads the job configuration and executes the pipeline.
    :param loader_config_file: The name of the job to execute.
    """
    try:      
        # Read loader configuration file
        with open(loader_config_file, "r") as file:
            loader_config = yaml.safe_load(file)  
        
        spark_app_name=loader_config["loader_parameters"]["spark_app_name"]    
        print(spark_app_name)
        source_url_query=loader_config["loader_parameters"]["source_url_query"]
            
            
        ## Get db connection uri
        # Get db configuration file path from loader configuration file
        conn_config_file=loader_config["loader_parameters"]["conn_config_file"]
        # Get db conn uri key path in configuration file
        conn_config_pgdb_uri_key=loader_config["loader_parameters"]["conn_config_pgdb_uri_key"]        
        # print(db_conn_uri_key)        
        # Read db configuration file to get db conn uri
        with open(conn_config_file, "r") as file:
            conn_config = yaml.safe_load(file)        
        pgdb_conn_uri = get_nested_dict(conn_config, conn_config_pgdb_uri_key)
        # print(pgdb_conn_uri)     


        conn_config_spark_key=loader_config["loader_parameters"]["conn_config_spark_key"]
        
        spark_conn_params = get_nested_dict(conn_config, conn_config_spark_key)
        print(spark_conn_params)

        
        ## Get iceberg raw table schema
        # Get schema configuration file path from loader configuration file
        schema_config_file=loader_config["loader_parameters"]['schema_config_file']
        # Get table key path in schema configuration file     
        iceberg_raw_table_key=loader_config["loader_parameters"]['schema_config_iceberg_raw_table_key']   
        iceberg_raw_table_name=iceberg_raw_table_key[1]        
        # Read schema configuration file to get table schema
        with open(schema_config_file, "r") as file:
            schema_config = yaml.safe_load(file)       
        iceberg_raw_table_definition = get_nested_dict(schema_config, iceberg_raw_table_key)

       
             

        # Initialize and execute the pipeline
        DataLoader = TradingViewLoader(
                 pgdb_conn_uri,
                 source_url_query,
                 spark_app_name, 
                 spark_conn_params,
                 iceberg_raw_table_name, 
                 iceberg_raw_table_definition                       
        )

        # # logger.info(f"Starting pipeline execution for job: {loader_config_file}")
        DataLoader.run_loader()
        # # logger.info(f"Pipeline execution completed successfully for job: {loader_config_file}")

    except FileNotFoundError as e:
        logger.critical(f"Configuration file missing: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred during job execution: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="Execute a data ingestion job."
    )
    parser.add_argument(
        "--loader-config-file",
        type=str,
        required=True,
        help="The name of the job to run (e.g., 'load_tradingview_data').",
    )

    # Parse arguments and execute the job
    args = parser.parse_args()
    main(args.loader_config_file)