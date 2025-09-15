import yaml
from pathlib import Path
import psycopg

class PgDBManager:
    def __init__(self, db_conn_params): 
        self.db_conn_params = db_conn_params
        self.conn = psycopg.connect(**self.db_conn_params)
        
    def get_sql_script_result_list(self, query):
        try:          
            with self.conn.cursor() as cursor:  # Cursor context
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
            with self.conn.cursor() as cursor:  # Cursor context
                # Execute query
                cursor.execute(query)
                # Fetch results
                results = cursor.fetchall()
            return results
        except Exception as e:
            print(f"An error occurred: {e}")
            return None

    # def execute_sql_script(self, sql_script):
    #     try:
    #         with psycopg2.connect(**self.db_conn_params) as conn:
    #             with conn.cursor() as cursor:  # Cursor context
    #                 cursor.execute(sql_script)
    #                 conn.commit()
    #     except Exception as e:
    #         print(f"An error occurred: {e}")

    def execute_sql_script(self, sql_script: str, params: tuple = ()) -> bool:
        """Execute a non-SELECT SQL script (e.g., INSERT, UPDATE, DELETE) with transaction handling.

        Args:
            sql_script (str): The SQL script to execute.
            params (tuple): Optional parameters for the script to prevent SQL injection.
        Returns:
            bool: True if the execution was successful, False otherwise.
        """
        try:
            with self.conn.cursor() as cursor:
                cursor.execute(sql_script, params)
                self.conn.commit()
                return True
        except psycopg.Error as e:
            print(f"Database error: {e}")
            self.conn.rollback()
            return False










    
    # def execute_sql_script(self, sql_script):
    #     try:
    #         print("234234324")
    #         with self.conn.cursor() as cursor:  # Cursor context
    #             print("asdfdsf")
    #             cursor.execute(sql_script)
    #             print(sql_script)
    #             conn.commit()
    #     except Exception as e:
    #         print(f"An error occurred: {e}")

# uri="postgresql://postgres:HDdimension1!@192.168.110.222:5432/finalytics"
# myPgDBManager= PgDBManager2(uri)
# query = "SELECT url FROM fin.vw_etl_tradingview_etf_component_spy WHERE etf_symbol ='SPY'"
# x=myPgDBManager.get_sql_script_result_list(query)
# print(x)




