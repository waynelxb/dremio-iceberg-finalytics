# Updated dockerfile to integrate Poetry and set the default shell to bash
# This is in fact to build spark35nb-poetry image!


# `alexmerced/spark35nb` Docker Image Documentation

## Overview

The `alexmerced/spark35nb` Docker image provides an environment for data engineers and data scientists to work with Apache Spark 3.5.2, Python 3.10, and JupyterLab. This image includes a comprehensive set of popular Python libraries for data processing, machine learning, and visualization. It is designed to run Apache Spark in a single-node mode alongside a JupyterLab server, making it ideal for development, testing, and educational purposes.

## Features

- **Apache Spark 3.5.2**: Pre-installed and configured to run in standalone single-node mode.
- **Python 3.10**: The latest version of Python, along with many essential data science libraries.
- **JupyterLab**: A web-based interactive development environment for Jupyter notebooks, code, and data.
- **Extensive Python Libraries**: Includes popular libraries such as `pandas`, `numpy`, `scikit-learn`, `tensorflow`, `pyspark`, `pyarrow`, `ibis-framework`, `dask`, and more.
- **Pre-configured Ports**: All necessary Spark and JupyterLab ports are exposed for easy access from your local machine.

## Included Libraries

This image comes with a wide range of pre-installed Python libraries, including but not limited to:

- **Data Manipulation**: `pandas`, `numpy`, `dask`, `polars`, `datafusion`
- **Machine Learning**: `scikit-learn`, `tensorflow`, `torch`, `xgboost`, `lightgbm`
- **Data Visualization**: `matplotlib`, `seaborn`, `plotly`
- **Big Data Tools**: `pyspark`, `pyarrow`, `ibis-framework`, `duckdb`, `sqlframe`
- **Data Ingestion**: `requests`, `beautifulsoup4`, `lxml`, `boto3`, `s3fs`, `minio`
- **Database Connectivity**: `sqlalchemy`, `psycopg2-binary`, `dremio-simple-query`

## Quick Start

### Pull the Docker Image

To pull the image from Docker Hub:

```sh
docker pull alexmerced/spark35nb
```

### Run the Container

```
docker run -p 8888:8888 -p 4040:4040 -p 7077:7077 -p 8080:8080 -p 18080:18080 -p 6066:6066 -p 7078:7078 -p 8081:8081 alexmerced/spark35nb
```

Then head over to localhost:8888 to access JupyterLab.

### Access Spark Web UIs

- **Spark Master Web UI**: [http://localhost:8081](http://localhost:8081)
- **Spark Worker Web UI**: [http://localhost:7078](http://localhost:7078)
- **Spark Application UI (default)**: [http://localhost:4040](http://localhost:4040)
- **Spark History Server**: [http://localhost:8080](http://localhost:8080) or [http://localhost:18080](http://localhost:18080)
- **Spark Standalone Mode REST Server**: [http://localhost:6066](http://localhost:6066)
