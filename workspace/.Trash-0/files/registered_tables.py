import yaml
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, IntegerType

class RegisteredTables:
    def __init__(self, zone, table, config_file_path):
        self.zone=zone
        self.table=table
        self.config_file_path=config_file_path
        
        with open(self.config_file_path, 'r') as f:
            config=yaml.safe_load(f)
        self.table_schema=config[zone][table]['schema']  
        self.table_partition_by=config[zone][table].get('partition_by',[])

    # Convert YAML schema to PySpark StructType
    def get_struct_type(self):
        # Map YAML types to PySpark types
        type_mapping = {
                        "StringType": StringType,
                        "FloatType": FloatType,
                        "IntegerType": IntegerType,
                    }        
        fields = [
            StructField(field["name"], type_mapping[field["type"]](), field["nullable"])
            for field in self.table_schema
        ]
        return StructType(fields)

    def get_schema_columns(self):
        schema_columns = ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in self.get_struct_type()])
        return schema_columns
    
    def get_partition_columns(self):
        partition_columns = ", ".join([p["field"] for p in self.table_partition_by]) if self.table_partition_by else ""
        return partition_columns
        
    def get_column_list(self):
        # Extract the 'name' attribute from each field in the schema
        column_list = [field['name'] for field in self.table_schema]
        return column_list
    
    def get_create_table_script(self):        
        # Create the Iceberg table
        script = f"""
        CREATE TABLE IF NOT EXISTS {self.table} ({self.get_schema_columns()})
        USING iceberg
        """
        if self.get_partition_columns():
            script += f" PARTITIONED BY ({self.get_partition_columns()})"         
        return script
   
# x=RegisteredTables('raw', 'raw.stock_eod_yfinance', 'table_schemas.yaml')    
# print(x.get_create_table_script())      
# print(x.get_partition_columns()) 
 