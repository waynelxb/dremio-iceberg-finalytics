import logging
import yaml
import pyspark
import pandas as pd
from pyspark.sql import SparkSession
from .schema_manager import SchemaManager
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType, TimestampType, LongType
# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class IcebergManager:    
    def __init__(self, spark_app_name, spark_conn_params):
        self.spark_app_name=spark_app_name                    
        self.catalog_uri = spark_conn_params['catalog_uri'] 
        self.warehouse = spark_conn_params['warehouse']     # Minio Address to Write to
        self.storage_uri = spark_conn_params['storage_uri']  # Minio IP address from docker inspec
        self.spark_master_uri = spark_conn_params['spark_master_uri']  # Minio IP address from docker inspec   
        self.spark_session = self._create_spark_session()
        
    def _create_spark_session(self)->SparkSession:
       try:  
            # with open(self.connection_config_file_path,"r") as file:
            #     config=yaml.safe_load(file)
            #     catalog_uri = config['spark']['catalog_uri'] 
            #     warehouse = config['spark']['warehouse']     # Minio Address to Write to
            #     storage_uri = config['spark']['storage_uri'] # Minio IP address from docker inspec
            #     spark_master_uri = config['spark']['spark_master_uri'] # Minio IP address from docker inspec           
            
            # Configure Spark with necessary packages and Iceberg/Nessie settings
            conf = (
                pyspark.SparkConf()
                    .setAppName(self.spark_app_name)
                    # Include necessary packages
                    .set('spark.jars.packages',
                         'org.postgresql:postgresql:42.7.3,'
                         'org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,'
                         'org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.77.1,'             
                         # awssdk 2.29.42 compatible with spark 3.5.4
                         'software.amazon.awssdk:bundle:2.24.8,'
                         'software.amazon.awssdk:url-connection-client:2.24.8')
                    # Enable Iceberg and Nessie extensions
                    .set('spark.sql.extensions', 
                         'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,'
                         'org.projectnessie.spark.extensions.NessieSparkSessionExtensions')
                    # Configure Nessie catalog
                    .set('spark.sql.catalog.nessie', 'org.apache.iceberg.spark.SparkCatalog')
                    .set('spark.sql.catalog.nessie.uri', self.catalog_uri)
                    .set('spark.sql.catalog.nessie.ref', 'main')
                    .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
                    .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
                    # Set Minio as the S3 endpoint for Iceberg storage
                    .set('spark.sql.catalog.nessie.s3.endpoint', self.storage_uri)
                    .set('spark.sql.catalog.nessie.warehouse', self.warehouse)
                    .set('spark.sql.catalog.nessie.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')
                    # Set master location, the job will be sent to the cluster
                    # .set('spark.master', spark_master_uri)
                    .set("spark.network.timeout", "50000s")
                    .set("spark.executor.heartbeatInterval", "60s")
                    .set("spark.task.maxFailures", "4") 
            )   
            
            # Start Spark session
            return SparkSession.builder.config(conf=conf).getOrCreate()
   
       except Exception as e:
            print(f"Error: {e}")
           
    def get_spark_session(self):
        return self.spark_session

    def create_spark_df_with_schema_dict(self, record_tuple_list, record_schema_dict):  
        schema = StructType([
            StructField(field["name"], eval(field["type"])(), field["nullable"])
            for field in record_schema_dict["schema"]
        ])
        spark_df = self.spark_session.createDataFrame(record_tuple_list, schema)
        return spark_df        

    def create_spark_df_with_schema(self, record_tuple_list, record_schema):  
        spark_df = self.spark_session.createDataFrame(record_tuple_list, record_schema)
        return spark_df   

    
    def create_iceberg_table(self, iceberg_table_name, create_iceberg_table_script):
        self.spark_session.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw;")  
        # Check if the Iceberg table exists, if not, create it
        if self.spark_session.catalog.tableExists(iceberg_table_name):            
            print(create_table_script)            
            self.spark_session.sql(create_iceberg_table_script)

    def truncate_iceberg_table(self, iceberg_table_name):      
        # Check if the Iceberg table exists and truncate it if it does
        if self.spark_session.catalog.tableExists(iceberg_table_name):
            self.spark_session.sql(f"TRUNCATE TABLE {iceberg_table_name}")
            logger.info(f"{iceberg_table_name} was loaded successfully.")
            print(f"Iceberg table {iceberg_table_name} was truncated successfully.")
        else:
            logger.info(f"Because the iceberg table {iceberg_table_name} does not exist, no truncation happened.")
            print(f"Iceberg table {iceberg_table_name} does not exist.")


    def load_data_from_df_into_iceberg(self, source_spark_df, target_iceberg_table, create_iceberg_table_script):
        try: 
            # Create namespace if not exists            
            self.spark_session.sql("CREATE NAMESPACE IF NOT EXISTS nessie.raw;")  
            # Check if the Iceberg table exists, if not, create it
            if self.spark_session.catalog.tableExists(target_iceberg_table)==False:
                logger.info(f"Since the iceberg table {target_iceberg_table} does not exist, it will be created.")
                # create_table_script = schema_manager.get_create_table_script("tables", target_iceberg_table)       
                self.spark_session.sql(create_iceberg_table_script) 
                
            source_spark_df.writeTo(target_iceberg_table).append()
            # # source_df.write.mode("overwrite").saveAsTable(iceberg_table)            
            incremental_count=source_spark_df.count()
            total_count=self.spark_session.table(target_iceberg_table).count()
            # logger.info(f"Since the iceberg table {target_iceberg_table} does not exist, it will be created.")
            print(f"{target_iceberg_table} was loaded with {incremental_count} records, totally {total_count} records.")
            
        except Exception as e:
            print(f"Error loading lceberg raw table: {e}")


    def load_data_from_iceberg_into_pg(self, jdbc_url, jdbc_conn_properties, source_iceberg_table, target_pg_table):
        try:             
            df_source=self.spark_session.read.table(source_iceberg_table)            
            # Write DataFrame to PostgreSQL
            df_source.write.jdbc(
                        url=jdbc_url,
                        table=target_pg_table,
                        mode="append",
                        properties=jdbc_conn_properties
                    )                
        except Exception as e:
            print(f"Error loading lceberg raw table: {e}")
            


# table_name="abc"
# table_def={'schema': [{'name': 'date', 'type': 'DateType', 'nullable': False}, {'name': 'symbol', 'type': 'StringType', 'nullable': False}, {'name': 'open', 'type': 'StringType', 'nullable': True}, {'name': 'high', 'type': 'StringType', 'nullable': True}, {'name': 'low', 'type': 'StringType', 'nullable': True}, {'name': 'close', 'type': 'StringType', 'nullable': True}, {'name': 'volume', 'type': 'IntegerType', 'nullable': True}, {'name': 'import_time', 'type': 'TimestampType', 'nullable': False}], 'partition_by': [{'field': 'date'}]}
# x=SparkTableManager(table_name, table_def)
# y=x.get_spark_table_schema()
# print(y)

