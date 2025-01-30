import pyspark
from pyspark.sql import SparkSession

class IcebergPgOperator:
    def __init__(self, spark_session: SparkSession, jdbc_url: str, jdbc_properties):
        self.spark_session = spark_session
        self.jdbc_url=jdbc_url
        self.jdbc_properties=jdbc_properties
        
    def insert_iceberg_data_into_pg(self, iceberg_source_table, pg_sink_table, mode):   
        try:    
            df_source=self.spark_session.read.table(iceberg_source_table)            
            # Write DataFrame to PostgreSQL
            df_source.write.jdbc(
                url=self.jdbc_url,
                table=pg_sink_table,
                mode=mode,
                properties=self.jdbc_properties
            )            
        except Exception as e:
            print(f"Error occured to insert_iceberg_data_into_pg: {e}") 