import yaml
import pyspark
from pyspark.sql import SparkSession
from .zz_schema_manager import SchemaManager


class IcebergManager:
    def __init__(self, connection_config_file_path, iceberg_schema_config_file_path, spark_app_name):
        self.spark_config_file = connection_config_file_path
        self.spark_app_name=spark_app_name
        self.iceberg_schema_config_file=iceberg_schema_config_file_path
        self.spark = self._create_spark_session()
        
    def _create_spark_session(self)->SparkSession:
       try:  
            with open(self.spark_config_file,"r") as file:
                config=yaml.safe_load(file)
                catalog_uri = config['spark']['catalog_uri'] 
                warehouse = config['spark']['warehouse']     # Minio Address to Write to
                storage_uri = config['spark']['storage_uri'] # Minio IP address from docker inspec
                spark_master_uri = config['spark']['spark_master_uri'] # Minio IP address from docker inspec
            
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
                    .set('spark.sql.catalog.nessie.uri', catalog_uri)
                    .set('spark.sql.catalog.nessie.ref', 'main')
                    .set('spark.sql.catalog.nessie.authentication.type', 'NONE')
                    .set('spark.sql.catalog.nessie.catalog-impl', 'org.apache.iceberg.nessie.NessieCatalog')
                    # Set Minio as the S3 endpoint for Iceberg storage
                    .set('spark.sql.catalog.nessie.s3.endpoint', storage_uri)
                    .set('spark.sql.catalog.nessie.warehouse', warehouse)
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
        return self.spark


    def insert_into_iceberg_table(self, spark_source_df, iceberg_sink_table):
        try: 
            schema_manager=SchemaManager(self.iceberg_schema_config_file)
            schema_struct_type=schema_manager.get_struct_type("tables", iceberg_sink_table)  
            
            create_table_script = schema_manager.get_create_table_query("tables", iceberg_sink_table)
            self.spark.sql(create_table_script)
         
            spark_source_df.writeTo(iceberg_sink_table).append()
            # source_spark_df.write.mode("overwrite").saveAsTable(iceberg_sink_table) 
    
            incremental_count=spark_source_df.count()
            total_count=self.spark.table(iceberg_sink_table).count()
    
            print(f"{iceberg_sink_table} was loaded with {incremental_count} records, totally {total_count} records.")
            
        except Exception as e:
            print(f"Error loading lceberg raw table: {e}")
    

    def truncate_iceberg_table(self, iceberg_table):      
        # Check if the Iceberg table exists and truncate it if it does
        if self.spark.catalog.tableExists(iceberg_table):
            self.spark.sql(f"TRUNCATE TABLE {iceberg_table}")
            print(f"Iceberg table {iceberg_table} truncated successfully.")
        else:
            print(f"Iceberg table {iceberg_table} does not exist.")

    