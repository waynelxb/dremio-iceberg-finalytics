import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import yaml
import argparse
from yahoo_records.load_yahoo_eod import LoadYahooEOD

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
        my_job = LoadYahooEOD(
            yahoo_api=job_params["yahoo_api"],
            equity_type=job_params["equity_type"],
            symbols_per_group=job_params["symbols_per_group"],
            connection_config_file_path=project_dir_path / job_params["connection_config_file"],
            schema_config_file_path=project_dir_path / job_params["schema_config_file"],
            spark_app_name=job_params["spark_app_name"],
        )
        my_job.get_eod_records()
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