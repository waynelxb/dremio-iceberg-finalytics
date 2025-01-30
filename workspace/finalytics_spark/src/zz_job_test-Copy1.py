import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Any
import yaml
# import pandas as pd
# from pyspark.sql import SparkSession
# from yahoo_records.zz_database_manager import PgDBManager
# from yahoo_records.zz_raw_yahoo import get_yahoo_raw_eod_records_consolidated
# from yahoo_records.zz_iceberg_manager import IcebergManager

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


# Main execution
if __name__ == "__main__":
    # Load configuration
    project_dir_path = Path(__file__).parent.parent
    config_file_path = project_dir_path / "config/cfg_jobs.yaml"
    config = load_config(config_file_path)

    # Extract job parameters
    job_params = config["jobs"]["load_yahoo_etf_eod_records"]["job_parameters"]
    # print(job_params)

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