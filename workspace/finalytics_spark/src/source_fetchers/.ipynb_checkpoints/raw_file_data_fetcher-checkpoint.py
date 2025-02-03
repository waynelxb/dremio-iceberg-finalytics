import pandas as pd
from datetime import date, timedelta, datetime
import time
import traceback
import os

class RawFileDataFetcher:
    def __init__(self, source_file_path, delimiter):
        self.source_file_path = source_file_path
        self.delimiter = delimiter

    def get_raw_file_data(self) -> pd.DataFrame:
        try:
            # Read the data from a file
            panda_df = pd.read_csv(self.source_file_path, sep=self.delimiter)
            panda_df["import_time"] = pd.to_datetime(datetime.now()).tz_localize(None)
            return panda_df    
        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()
            return pd.DataFrame()
 