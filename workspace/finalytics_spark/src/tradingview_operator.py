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
      

def main(operator_config_file: str):
    """
    Main function that loads the job configuration and executes the pipeline.
    :param operator_config_file: The name of the job to execute.
    """
    try: 
        ######## Get parameters required by TradingViewLoader
        
        # Read operator_config_file
        with open(operator_config_file, "r") as file:
            operator_config = yaml.safe_load(file)  
        
        spark_app_name=operator_config["parameters"]["spark_app_name"]    
        source_url_query=operator_config["parameters"]["source_url_query"]            
            
        ## Get db connection and spark config from connection configuration file
        # Get connection configuration file path
        conn_config_file=operator_config["parameters"]["conn_config_file"]
        
        # 1.1 Get db conn uri key path from operatior configuration file
        conn_config_pgdb_uri_key=operator_config["parameters"]["conn_config_pgdb_uri_key"]  
        # 1.2 Read db configuration file to get pgdb_conn_uri
        with open(conn_config_file, "r") as file:
            conn_config = yaml.safe_load(file)        
        pgdb_conn_uri = get_nested_dict(conn_config, conn_config_pgdb_uri_key)
        
        # 2.1 Get spark configuration key path from operatior configuration file       
        conn_config_spark_key=operator_config["parameters"]["conn_config_spark_key"]
        # 2.2 Get spark configuration params   
        spark_conn_params = get_nested_dict(conn_config, conn_config_spark_key)
        print(spark_conn_params)

        
        ## Get iceberg raw table schema
        # Get schema configuration file path from operator configuration file
        schema_config_file=operator_config["parameters"]['schema_config_file']
        
        # Get table key path in schema configuration file     
        iceberg_raw_table_key=operator_config["parameters"]['schema_config_iceberg_raw_table_key']   
        iceberg_raw_table_name=iceberg_raw_table_key[1]        
        # Read schema configuration file to get table schema
        with open(schema_config_file, "r") as file:
            schema_config = yaml.safe_load(file)       
        iceberg_raw_table_definition = get_nested_dict(schema_config, iceberg_raw_table_key)

        print(len(iceberg_raw_table_definition['schema']))

        # Initialize and execute the pipeline
        DataLoader = TradingViewLoader(
                 pgdb_conn_uri,
                 source_url_query,
                 spark_app_name, 
                 spark_conn_params,
                 iceberg_raw_table_name, 
                 iceberg_raw_table_definition                       
        )

        # # # logger.info(f"Starting pipeline execution for job: {operator_config_file}")
        DataLoader.run_loader()
        # # # logger.info(f"Pipeline execution completed successfully for job: {operator_config_file}")

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
        "--operator-config-file",
        type=str,
        required=True,
        help="The name of the job to run (e.g., 'load_tradingview_data').",
    )

    # Parse arguments and execute the job
    args = parser.parse_args()
    main(args.operator_config_file)