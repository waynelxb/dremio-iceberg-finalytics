import yaml
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType, TimestampType, LongType

class SparkSchemaBasedScriptGenerator:
    def __init__(self, spark_table_name, spark_table_def):
        self.spark_table_name = spark_table_name
        self.spark_table_def = spark_table_def         
    
    def get_spark_dataframe_schema(self):
        schema = StructType([
            StructField(field["name"], eval(field["type"])(), field["nullable"])
            for field in self.spark_table_def["schema"]
        ])
        return schema    

    def get_column_list(self):
        column_list = [field['name'] for field in self.spark_table_def["schema"]]
        # elif object_type == "apis":
        #     column_list = object_def
        return column_list
    
    def get_create_spark_table_script(self):    
        schema = self.get_spark_dataframe_schema()        
        # Generate SQL columns
        columns = ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in schema.fields])     
        
        # Generate CREATE TABLE query
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {self.spark_table_name} ({columns})
        """
        
        if 'partition_by' in schema:
            partition_by = self.spark_table_def['partition_by']
            partitioning = ", ".join([p["field"] for p in partition_by]) if partition_by else ""
            create_table_query += f" PARTITIONED BY ({partitioning})"
        
        return create_table_query.strip()

# table_name="abc"
# table_def={'schema': [{'name': 'date', 'type': 'DateType', 'nullable': False}, {'name': 'symbol', 'type': 'StringType', 'nullable': False}, {'name': 'open', 'type': 'StringType', 'nullable': True}, {'name': 'high', 'type': 'StringType', 'nullable': True}, {'name': 'low', 'type': 'StringType', 'nullable': True}, {'name': 'close', 'type': 'StringType', 'nullable': True}, {'name': 'volume', 'type': 'IntegerType', 'nullable': True}, {'name': 'import_time', 'type': 'TimestampType', 'nullable': False}], 'partition_by': [{'field': 'date'}]}
# x=SparkTableManager(table_name, table_def)
# y=x.get_spark_table_schema()
# print(y)

