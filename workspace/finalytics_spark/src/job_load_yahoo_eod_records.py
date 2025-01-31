import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import yaml
import argparse
from yahoo_records.curated_yahoo_data_ingester import CuratedYahooDataIngester

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_config(config_file_path: Path) -> Dict[str, Any]:
    """
    Load configuration from a YAML file.

    Args:
        config_file_path (Path): Path to the YAML configuration file.

    Returns:
        Dict[str, Any]: Configuration parameters.
    """
    try:
        with open(config_file_path, "r") as file:
            config = yaml.safe_load(file)
        return config
    except Exception as e:
        logger.error(f"Failed to load configuration file: {e}", exc_info=True)
        raise


def main(job_name: str):
    """
    Main function to load configuration and run the job.

    Args:
        job_name (str): The name of the job to run.
    """
    try:
        # Load configuration
        project_dir_path = Path(__file__).parent.parent
        config_file_path = project_dir_path / "config/cfg_jobs.yaml"
        config = load_config(config_file_path)

        # Extract job parameters
        if job_name not in config["jobs"]:
            raise ValueError(f"Job '{job_name}' not found in the configuration file.")
        job_params = config["jobs"][job_name]["job_parameters"]

        # Initialize and run the job
        my_job = CuratedYahooDataIngester(
            record_type=job_params["record_type"],
            equity_type=job_params["equity_type"],
            query_grouped_symbol=job_params["query_grouped_symbol"],
            connection_config_file_path=project_dir_path / job_params["connection_config_file"],
            schema_config_file_path=project_dir_path / job_params["schema_config_file"],
            spark_app_name=job_params["spark_app_name"],
            iceberg_raw_table=job_params["iceberg_raw_table"],
            pg_stage_table=job_params["pg_stage_table"],
            script_merge_pg_stage_into_fin=job_params["script_merge_pg_stage_into_fin"],
        )
        my_job.ingest_yahoo_data()
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(description="Run a job based on the provided job name.")
    parser.add_argument(
        "--job-name",
        type=str,
        required=True,
        help="The name of the job to run (e.g., 'load_yahoo_etf_eod_records').",
    )
    args = parser.parse_args()

    # Run the main function with the provided job name
    main(args.job_name)