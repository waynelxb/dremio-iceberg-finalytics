import os
import yaml
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from zone_raw_yfinance import RawYFIngestion
from xx import MyModel





# Function to process the records (pass through parameters)
def process_yfinance_record(single_param_pair):
    # print(f"Processing {single_param_pair}")
    return self.fetch_yfinance_record(single_param_pair)

# Parallel fetch function
def parallel_fetch(spark, multi_param_pairs):        

    # Create RDD from the input parameter pairs
    record_rdd = self.spark.sparkContext.parallelize(multi_param_pairs)

    # Use flatMap to return a flattened list of records
    results_rdd = record_rdd.flatMap(process_yfinance_record)

    # Collect the results from the RDD and convert to a list of tuples
    # results = results_rdd.collect()        
    df = spark.createDataFrame(results_rdd, self.registered_column_list)   
    df.show()
    return df



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
            .set('spark.jars.packages', 'org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8')
            # Enable Iceberg and Nessie extensions
            .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
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
    )   
    
    # Start Spark session
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("Spark Session Started")
    # model = MyModel(spark)
    # model.run()
    raw_stock_eod_yfinance_ingestion = RawYFIngestion(spark, 'stock', 'raw', 'raw.stock_eod_yfinance', 'registered_table_schemas.yaml')
    # raw_stock_eod_yfinance_ingestion.run()
    yf_param_pairs = [
        ('AAPL', '2024-12-10'),
        ('MSFT', '2024-12-10'),
        ('GOOGL', '2024-12-10'),
    ]
    raw_stock_eod_yfinance_ingestion.parallel_fetch(yf_param_pairs)
 
   

#         # List of stock symbols and start dates
#     yf_param_pairs = [
#         ('AAPL', '2024-12-10'),
#         ('MSFT', '2024-12-10'),
#         ('GOOGL', '2024-12-10'),
#     ]

#     # Instantiate the class
#     stock_stage = RawYFIngestion(spark, 'stock', 'raw', 'raw.stock_eod_yfinance', 'registered_table_schemas.yaml')

#     # Fetch data in parallel
#     stock_data_rows = stock_stage.parallel_fetch(yf_param_pairs)
  

if __name__=="__main__":
    main()
    

    