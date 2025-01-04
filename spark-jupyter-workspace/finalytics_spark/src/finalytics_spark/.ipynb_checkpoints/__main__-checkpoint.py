import os
import yaml
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from raw_yfinance_ingestion import RawYFIngestion
from xx import MyModel

def main():    
    with open("environment.yaml","r") as file_object:
        documents=yaml.safe_load_all(file_object)
        for doc in documents:
            doc_name = doc['document_name']
            if doc_name=='iceberg_env':
                CATALOG_URI = doc['catalog_uri'] # Nessie Server URI
                WAREHOUSE = doc['warehouse']     # Minio Address to Write to
                STORAGE_URI = doc['storage_uri'] # Minio IP address from docker inspec


    # Configure Spark with necessary packages and Iceberg/Nessie settings
    conf = (
        pyspark.SparkConf()
            .setAppName('finalytics_app')
            # Include necessary packages
            .set('spark.jars.packages',
                 'org.postgresql:postgresql:42.7.3,'
                 'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
                 'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,'
                 'software.amazon.awssdk:bundle:2.24.8,'
                 'software.amazon.awssdk:url-connection-client:2.24.8')
            # Enable Iceberg and Nessie extensions
            .set('spark.sql.extensions', 
                 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
                 'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
            # Configure Nessie catalog
            .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
            .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
            .set('spark.sql.catalog.nessie.ref', 'main')
            .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
            .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
            # Set Minio as the S3 endpoint for Iceberg storage
            .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
            .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
            .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')       
        
            .set("spark.sql.catalog.finalytics", "org.apache.iceberg.spark.SparkCatalog") \
            .set("spark.sql.catalog.finalytics.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog") \
            .set("spark.sql.catalog.finalytics.warehouse", "s3://bucket/catalog1/warehouse") \
            .set("spark.sql.catalog.finalytics.uri", "http://localhost:19120/api/v1") \
            .set("spark.sql.catalog.finalytics.ref", "dev") \
    )   
    
    # Start Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()   

    raw_eod_yfinance = RawYFIngestion(spark, 'stock', 'raw', 'raw.stock_eod_yfinance', 'registered_table_schemas.yaml')
    yf_param_pairs = [
        ('AAPL', '2024-12-10'),
        ('MSFT', '2024-12-10'),
        ('GOOGL', '2024-12-10'),
    ]
   
    raw_eod_yfinance.parallel_fetch(yf_param_pairs)

if __name__=="__main__":
    main()
    

    