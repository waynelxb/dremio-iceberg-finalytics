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
        
        spark_app_name=loader_config["loader_parameters"]["spark_app_name"],            
        tradingview_url_query=loader_config["loader_parameters"]["tradingview_url_query"]
            
            
        ## Get db connection uri
        # Get db configuration file path from loader configuration file
        db_config_file=loader_config["loader_parameters"]['conn_config_file']
        # Get db conn uri key path in configuration file
        db_conn_uri_key=loader_config["loader_parameters"]['conn_config_db_uri_key']        
        # print(db_conn_uri_key)
        
        # Read db configuration file to get db conn uri
        with open(db_config_file, "r") as file:
            db_config = yaml.safe_load(file)        
        db_conn_uri = get_nested_dict(db_config, db_conn_uri_key)
        print(db_conn_uri)     

       
        ## Get iceberg raw table schema
        # Get schema configuration file path from loader configuration file
        schema_config_file=loader_config["loader_parameters"]['schema_config_file']
        # Get table key path in schema configuration file     
        iceberg_raw_table_key=loader_config["loader_parameters"]['schema_config_iceberg_raw_table_key']    
        iceberg_raw_table=iceberg_raw_table_key[1]
        
        # Read schema configuration file to get table schema
        with open(schema_config_file, "r") as file:
            schema_config = yaml.safe_load(file)       
        iceberg_raw_table_schema = get_nested_dict(schema_config, iceberg_raw_table_key)
        # print(iceberg_raw_table_schema) 


        
        # loader_config = load_config(Path(loader_config_file))
        # print(loader_config)

        # loader_params = loader_config.get("loader_parameters", {})
        # print(loader_params)
        # if not loader_params:
        #     logger.error(f"Missing 'loader_parameters' for job '{loader_config_file}'.")
        #     raise ValueError(f"Missing 'loader_parameters' for job '{loader_config_file}'.")        

        # Initialize and execute the pipeline
        DataLoader = TradingViewLoader(
                 db_conn_uri,
                 iceberg_raw_table, 
                 iceberg_raw_table_schema,
                 spark_app_name,                 
                 tradingview_url_query        
        )

        # logger.info(f"Starting pipeline execution for job: {loader_config_file}")
        DataLoader.run_loader()
        # logger.info(f"Pipeline execution completed successfully for job: {loader_config_file}")

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


