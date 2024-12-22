import os
import yaml
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

with open("config.yaml","r") as file_object:
    documents=yaml.safe_load_all(file_object)
    for doc in documents:
        doc_name = doc['document_name']
        if doc_name=='yfinance_stock':
            registered_col_list=doc['column_list']
            registered_col_list.sort()
            print(registered_col_list)
        if doc_name=='iceberg_env':
            CATALOG_URI = doc['catalog_uri'] # Nessie Server URI
            WAREHOUSE = doc['warehouse']     # Minio Address to Write to
            STORAGE_URI = doc['storage_uri'] # Minio IP address from docker inspec


# ## DEFINE SENSITIVE VARIABLES
# CATALOG_URI = config_dict["iceberg_env"]["CATALOG_URI"]    # Nessie Server URI
# WAREHOUSE = config_dict["iceberg_env"]["WAREHOUSE"]               # Minio Address to Write to
# STORAGE_URI = config_dict["iceberg_env"]["STORAGE_URI"]      # Minio IP address from docker inspect

# # Configure Spark with necessary packages and Iceberg/Nessie settings
# conf = (
#     pyspark.SparkConf()
#         .setAppName('sales_data_app')
#         # Include necessary packages
#         .set('spark.jars.packages', 'org.postgresql:postgresql:42.7.3,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,software.amazon.awssdk:bundle:2.24.8,software.amazon.awssdk:url-connection-client:2.24.8')
#         # Enable Iceberg and Nessie extensions
#         .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
#         # Configure Nessie catalog
#         .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
#         .set('spark.sql.catalog.nessie.uri', CATALOG_URI)
#         .set('spark.sql.catalog.nessie.ref', 'main')
#         .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
#         .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
#         # Set Minio as the S3 endpoint for Iceberg storage
#         .set('spark.sql.catalog.nessie.s3.endpoint', STORAGE_URI)
#         .set('spark.sql.catalog.nessie.warehouse', WAREHOUSE)
#         .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
# )

def main():
    
    # Start Spark session
    # spark = SparkSession.builder.config(conf=conf).getOrCreate()
    print("Spark Session Started")
    
    # # Define a schema for the sales data
    # schema = StructType([
    #     StructField("order_id", IntegerType(), True),
    #     StructField("customer_id", IntegerType(), True),
    #     StructField("product", StringType(), True),
    #     StructField("quantity", IntegerType(), True),
    #     StructField("price", DoubleType(), True),
    #     StructField("order_date", StringType(), True)
    # ])
    
    # # Create a DataFrame with messy sales data (including duplicates and errors)
    # sales_data = [
    #     (1, 101, "Laptop", 1, 1000.00, "2023-08-01"),
    #     (2, 102, "Mouse", 2, 25.50, "2023-08-01"),
    #     (3, 103, "Keyboard", 1, 45.00, "2023-08-01"),
    #     (1, 101, "Laptop", 1, 1000.00, "2023-08-01"),  # Duplicate
    #     (4, 104, "Monitor", None, 200.00, "2023-08-02"),  # Missing quantity
    #     (5, None, "Mouse", 1, 25.50, "2023-08-02")  # Missing customer_id
    # ]
    
    # # Convert the data into a DataFrame
    # sales_df = spark.createDataFrame(sales_data, schema)
    
    # # Create the "sales" namespace
    # spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.sales;").show()
    
    # # Write the DataFrame to an Iceberg table in the Nessie catalog
    # sales_df.writeTo("nessie.sales.sales_data_raw").createOrReplace()
    
    # # Verify by reading from the Iceberg table
    # spark.read.table("nessie.sales.sales_data_raw").show()
    
    # Stop the Spark session
    # spark.stop()
if __name__=="__main__":
    main()
    
