import logging
import argparse
from pathlib import Path
from typing import Dict, Any
import yaml

from etl_pipelines.iceberg_to_pg_pipeline import IcebergToPgPipeline

# Configure logging with timestamps
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def load_config(config_file_path: Path) -> Dict[str, Any]:
    """
    Loads a YAML configuration file.

    :param config_file_path: Path to the configuration file.
    :return: Dictionary containing configuration data.
    :raises FileNotFoundError: If the configuration file does not exist.
    :raises Exception: If any other error occurs while loading the config.
    """
    if not config_file_path.exists():
        logger.error(f"Configuration file not found: {config_file_path}")
        raise FileNotFoundError(f"Configuration file not found: {config_file_path}")

    try:
        with open(config_file_path, "r") as file:
            config = yaml.safe_load(file)
        logger.info("Configuration file loaded successfully.")
        return config
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML configuration file: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to load configuration file: {e}", exc_info=True)
        raise


def main(job_name: str):
    """
    Main function that loads the job configuration and executes the pipeline.

    :param job_name: The name of the job to execute.
    """
    try:
        # Determine the project directory and configuration file path
        project_dir_path = Path(__file__).resolve().parent.parent
        config_file_path = project_dir_path / "config/cfg_jobs.yaml"

        # Load the configuration file
        job_config = load_config(config_file_path)

        # Validate job name
        jobs = job_config.get("jobs", {})
        if job_name not in jobs:
            logger.error(f"Job '{job_name}' not found in the configuration file.")
            raise ValueError(f"Job '{job_name}' not found in the configuration file.")

        job_params = jobs[job_name].get("job_parameters", {})
        if not job_params:
            logger.error(f"Missing 'job_parameters' for job '{job_name}'.")
            raise ValueError(f"Missing 'job_parameters' for job '{job_name}'.")

        # Initialize and execute the pipeline
        pipeline = IcebergToPgPipeline(
            connection_config_file_path=project_dir_path / job_params["connection_config_file"],
            schema_config_file_path=project_dir_path / job_params["schema_config_file"],           
            spark_app_name=job_params["spark_app_name"],
            iceberg_raw_table=job_params["iceberg_raw_table"],
            pg_stage_table=job_params["pg_stage_table"],
            script_merge_pg_stage_into_fin=job_params["script_merge_pg_stage_into_fin"]
        )

        logger.info(f"Starting pipeline execution for job: {job_name}")
        pipeline.execute_pipeline()
        logger.info(f"Pipeline execution completed successfully for job: {job_name}")

    except FileNotFoundError as e:
        logger.critical(f"Configuration file missing: {e}")
        raise
    except Exception as e:
        logger.error(f"An error occurred during job execution: {e}", exc_info=True)
        raise


if __name__ == "__main__":
    # Set up argument parsing
    parser = argparse.ArgumentParser(
        description="Execute a data ingestion job from the configuration file."
    )
    parser.add_argument(
        "--job-name",
        type=str,
        required=True,
        help="The name of the job to run (e.g., 'load_yahoo_etf_eod_records').",
    )

    # Parse arguments and execute the job
    args = parser.parse_args()
    main(args.job_name)
