from yahooquery import Ticker
import pandas as pd
from datetime import date, timedelta
from collections import defaultdict
import random
import time
import warnings
import traceback
import numpy as np
import os

class RawSADataCollector:
    def __init__(self, equity_type, source_file_path, delimiter):
        self.equity_type = equity_type
        self.source_file_path = source_file_path
        self.delimiter = delimiter

        if self.equity_type not in ("stock", "etf"):
            raise ValueError(f"Invalid value: {self.equity_type}. Expected one of ('stock', 'etf').")


    def get_raw_sa_symbols(self) -> pd.DataFrame:
        try:
            # Read the data from a file
            panda_df = pd.read_csv(self.source_file_path, sep=self.delimiter, header=None)
            if self.equit=="stock":
                panda_df.columns = ['symbol', 'company_name', 'industry', 'market_cap'] 
            elif self.equit=="etf":  
                panda_df.columns = ['symbol', 'assets', 'fund_name', 'asset_class']                   
            return panda_df
    
        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()
            return pd.DataFrame()
 