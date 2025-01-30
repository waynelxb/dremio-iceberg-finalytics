from pathlib import Path
import yaml

def load_config(config_file_path: Path):
    with open(config_file_path, "r") as file:
        return yaml.safe_load(file)

# Load the YAML configuration
config_file_path = Path("cfg_jobs.yaml")  # Adjust the path if needed
config = load_config(config_file_path)

# Access the job's parameters
job_name = "load_yahoo_etf_eod_records"
if job_name in config["jobs"]:
    job_params = config["jobs"][job_name]["job_parameters"]
    yahoo_api = job_params["yahoo_api"]
    print(f"Yahoo API for '{job_name}': {yahoo_api}")
else:
    print(f"Job '{job_name}' not found in the configuration.")