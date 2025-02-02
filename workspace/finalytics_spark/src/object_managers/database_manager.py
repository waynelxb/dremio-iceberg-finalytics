import psycopg2
import yaml
from pathlib import Path


class PgDBManager:

    def __init__(self, config_file_path):        
        self.config_file_path = config_file_path      
        # Load YAML configuration
        with open(self.config_file_path, 'r') as file:
            config = yaml.safe_load(file)
            pg_config= config['databases']['postgresql']['finalytics']
            
            self.db_conn_params = {
                        "host": pg_config["host"],
                        "port": pg_config["port"],
                        "user": pg_config["user"],
                        "password": pg_config["password"],
                        "database": pg_config["database"]  # psycopg2 uses "dbname" instead of "database"
                    }           
            self.jdbc_url = f"jdbc:postgresql://{pg_config['host']}:{pg_config['port']}/{pg_config['database']}"
            self.jdbc_properties= {
                "user": pg_config['user'],
                "password": pg_config['password'],
                "driver": pg_config["driver"]}    

    def get_sql_script_result_list(self, query):
        try:
            with psycopg2.connect(**self.db_conn_params) as conn:  # Connection context
                with conn.cursor() as cursor:  # Cursor context
                    # Execute query
                    cursor.execute(query)
                    # Fetch results
                    results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    def get_sql_script_result(self, query):
        try:
            with psycopg2.connect(**self.db_conn_params) as conn:  # Connection context
                with conn.cursor() as cursor:  # Cursor context
                    # Execute query
                    cursor.execute(query)
                    # Fetch results
                    results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    
    def execute_sql_script(self, sql_script):
        try:
            with psycopg2.connect(**self.db_conn_params) as conn:
                with conn.cursor() as cursor:  # Cursor context
                    cursor.execute(sql_script)
                    conn.commit()
        except Exception as e:
            print(f"An error occurred: {e}")
