import yaml
import pyspark
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, DateType, FloatType, TimestampType

class SchemaManager:
    def __init__(self, config_file_path):
        self.config_file_path = config_file_path
        self.config = self._load_config()
        
    def _load_config(self):
        try:
            with open(self.config_file_path, 'r') as file:
                return yaml.safe_load(file)
        except FileNotFoundError:
            raise FileNotFoundError(f"Configuration file '{self.config_file_path}' not found.")
        except yaml.YAMLError as e:
            raise ValueError(f"Error parsing YAML file: {e}")
    
    def get_object_config(self, object_type, object_name):
        try:
            return self.config[object_type][object_name]
        except KeyError as e:
            raise KeyError(f"{e.args[0]} not found in the configuration.")
         
    
    def get_object_schema(self, object_type, object_name):
    
        """
        Generate a PySpark schema for a given table name from the configuration.
        
        :param table_name: Name of the table in the configuration.
        :return: A PySpark StructType schema.
        """
#         if table_name not in self.config["tables"]:
#             raise ValueError(f"Table '{table_name}' not found in the configuration.")    

        object_config = self.get_object_config(object_type, object_name)
        if object_type=="tables":
            schema = StructType([
                StructField(field["name"], eval(field["type"])(), field["nullable"])
                for field in object_config["schema"]
            ])
            return schema    

    def get_column_list(self, object_type, object_name):
        # Extract the 'name' attribute from each field in the schema
        object_config = self.get_object_config(object_type, object_name)
        
        if object_type == "tables":
            schema_config = object_config["schema"]
            column_list = [field['name'] for field in schema_config]
        elif object_type == "apis":
            column_list = object_config
        return column_list
    
    def get_create_table_query(self, object_type, object_name):
        if object_type=="tables":
            object_config = self.get_object_config(object_type, object_name)           
            object_schema = self.get_object_schema(object_type, object_name)
            partition_by = object_config.get("partition_by", [])
            
            # Generate SQL columns
            columns = ", ".join([f"{field.name} {field.dataType.simpleString()}" for field in object_schema.fields])
            partitioning = ", ".join([p["field"] for p in partition_by]) if partition_by else ""
            
            # Generate CREATE TABLE query
            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS {object_name} ({columns})
            """
            if partitioning:
                create_table_query += f" PARTITIONED BY ({partitioning})"
            return create_table_query.strip()



