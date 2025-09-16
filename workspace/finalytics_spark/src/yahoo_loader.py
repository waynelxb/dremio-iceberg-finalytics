import logging
import argparse
from pathlib import Path
from typing import Dict, Any
import yaml
from functools import reduce
import operator

from pipelines.yahoo_pipeline import YahooPipeline

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


def main(loader_config_file: str, assignment: str):
    """
    Main function that loads the job configuration and executes the pipeline.
    :param loader_config_file: The name of the job to execute.
    """
    try: 
        ######## Get parameters required by TradingViewLoader
        
        # Read loader_config_file
        with open(loader_config_file, "r") as file:
            loader_config = yaml.safe_load(file)  
        
        spark_app_name=loader_config['assignments'][assignment]["parameters"]["spark_app_name"]    
        grouped_symbol_query=loader_config['assignments'][assignment]["parameters"]["grouped_symbol_query"]            
        record_type=loader_config['assignments'][assignment]["parameters"]["record_type"] 
        pg_stage_table_name=loader_config['assignments'][assignment]["parameters"]['pg_stage_table_name'] 


        ## Get db connection and spark config from connection configuration file
        # 1.1 Get connection configuration file path        
        conn_config_file=loader_config['assignments'][assignment]["parameters"]["conn_config_file"]
        # 1.2 Read db configuration file to get pgdb_conn_uri
        with open(conn_config_file, "r") as file:
            conn_config = yaml.safe_load(file)    
        # 1.3 Get db conn params key path from operatior configuration file
        conn_config_pgdb_uri_key=loader_config['assignments'][assignment]["parameters"]["conn_config_pgdb_params_key"]  
        pgdb_conn_params = get_nested_dict(conn_config, conn_config_pgdb_uri_key)
               
        # 2.1 Get spark configuration key path from operatior configuration file       
        conn_config_spark_key=loader_config['assignments'][assignment]["parameters"]["conn_config_spark_key"]
        # 2.2 Get spark configuration params   
        spark_conn_params = get_nested_dict(conn_config, conn_config_spark_key)
                
        ## Get iceberg raw table schema
        # Get schema configuration file path from loader configuration file
        schema_config_file=loader_config['assignments'][assignment]["parameters"]['schema_config_file']
        
        # Get table key path in schema configuration file     
        iceberg_raw_table_key=loader_config['assignments'][assignment]["parameters"]['schema_config_iceberg_raw_table_key']   
        iceberg_raw_table_name=iceberg_raw_table_key[1]   
   
        # Read schema configuration file to get table schema
        with open(schema_config_file, "r") as file:
            schema_config = yaml.safe_load(file)       
        iceberg_raw_table_definition = get_nested_dict(schema_config, iceberg_raw_table_key)
   

        # Initialize and execute the pipeline
        yahoo_pipeline = YahooPipeline(
                pgdb_conn_params,
                grouped_symbol_query,
                record_type,
                spark_app_name, 
                spark_conn_params,
                iceberg_raw_table_name, 
                iceberg_raw_table_definition,
                pg_stage_table_name                      
        )

        # # # logger.info(f"Starting pipeline execution for job: {loader_config_file}")
        yahoo_pipeline.execute_pipeline()
        # # # logger.info(f"Pipeline execution completed successfully for job: {loader_config_file}")

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
        help="loader config file path, e.g., '/opt/workspace/finalytics_spark/config/cfg_yahoo_loader.yaml'.",
    )

    parser.add_argument(
        "--assignment",
        type=str,
        required=True,
        help="the assignment of the job run, e.g., 'load_yahoo_etf_eod_quotes_to_iceberg'.",
    )

    # Parse arguments and execute the job
    args = parser.parse_args()
    main(args.loader_config_file, args.assignment)

# (finalytics-spark-py3.11) root@9607765a4290:/opt/workspace/finalytics_spark/src# python yahoo_loader.py --loader-config-file /opt/workspace/finalytics_spark/config/cfg_yahoo_loader.yaml --assignment load_yahoo_etf_eod_quotes_to_iceberg