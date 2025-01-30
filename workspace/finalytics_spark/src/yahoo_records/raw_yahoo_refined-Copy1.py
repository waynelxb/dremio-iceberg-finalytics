from yahooquery import Ticker
import yfinance as yf
import pandas as pd
from datetime import datetime, date, timedelta
from collections import defaultdict
import random
import time
import warnings
import traceback

# Suppress FutureWarning from yahooquery
class RawYahoo:
    def __init__(self, subject):
        self.subject=subject
        

warnings.filterwarnings("ignore", category=FutureWarning, module="yahooquery")

def get_raw_eod_quote(symbol_list: list, start_date: date) -> pd.DataFrame:
    """
    Fetch end-of-day (EOD) records for a list of symbols using either yahooquery or yfinance.

    Args:
        api (str): The API to use ("yahooquery" or "yfinance").
        symbol_list (list): List of stock symbols.
        start_date (date): Start date for fetching historical data.

    Returns:
        pd.DataFrame: DataFrame containing EOD records.
    """
    try:
        # To avoid time zone issues, set end_date to tomorrow
        end_date = date.today() + timedelta(days=1)

        # Fetch historical data using yahooquery
        data = Ticker(symbol_list)
        hist_data = data.history(start=start_date, end=end_date, interval="1d")
        hist_data = hist_data.reset_index()

        
        # Select and format required columns
        columns_to_select = ["date", "symbol", "open", "high", "low", "close", "volume"]
        hist_data = hist_data[columns_to_select]
        hist_data["date"] = hist_data["date"].astype(str).str.slice(0, 10)
        hist_data["date"] = pd.to_datetime(hist_data["date"])
                                           
        return hist_data
    
    except Exception as e:
        # Handle exceptions and return an empty DataFrame
        print(f"An error occurred: {e}, {symbol_list}, {start_date}")
        traceback.print_exc()  # Print detailed traceback for debugging
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])


def get_raw_eod_quote_consolidated(group_date_symbol_list: list) -> pd.DataFrame:
    """
    Fetch and consolidate EOD records for grouped symbols.

    Args:
        api (str): The API to use ("yahooquery" or "yfinance").
        group_date_symbol_list (list): List of tuples containing (group_id, group_start_date, symbol).

    Returns:
        pd.DataFrame: Consolidated DataFrame of EOD records.
    """
    try:
        if len(group_date_symbol_list)>0:
            largest_group_id = max(group_date_symbol_list, key=lambda x: x[0])[0]
            grouped_symbols = defaultdict(list)
            
            # Group symbols by (group_id, group_start_date)
            for group_id, group_start_date, symbol in group_date_symbol_list:
                grouped_symbols[(group_id, group_start_date)].append(symbol)
            
            stacked_hist_group_panda_dfs = []
            for (group_id, group_start_date), group_symbols in grouped_symbols.items():
                print(f"Processing group {group_id}/{largest_group_id} with {len(group_symbols)} symbols...")
                hist_group_panda_df = get_raw_eod_quote(group_symbols, group_start_date)
                print(f"Retrieved {len(hist_group_panda_df)} records for group {group_id}/{largest_group_id}.")
                
                stacked_hist_group_panda_dfs.append(hist_group_panda_df)
                sleep_time = random.randint(1, 5)
                print(f"Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)
            
            # Combine all DataFrames into one
            consolidated_hist_panda_df = pd.concat(stacked_hist_group_panda_dfs, ignore_index=True)
        else: 
            consolidated_hist_panda_df = pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])
            
        return consolidated_hist_panda_df
    
    except Exception as e:
        # Handle exceptions and return an empty DataFrame
        print(f"An error occurred: {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])



def get_raw_market_quote(symbol_list: list) -> pd.DataFrame:
    """
    Fetch instant market data for a list of symbols.

    Args:
        symbol_list (list): List of stock symbols.

    Returns:
        pd.DataFrame: DataFrame containing instant market data.
    """
    try:
        # Fetch data using yahooquery
        t = Ticker(symbol_list)
        prices = t.price  # Access price data

        # Extract data into a dictionary
        market_data = {
            symbol: {
                "exchange_name": details.get("exchangeName", "NULL"),
                "current_market_state": details.get("marketState", "NULL"),
                "pre_market_time": details.get("preMarketTime", "NULL"),
                "pre_market_price": details.get("preMarketPrice", "NULL"),
                "pre_market_change": details.get("preMarketChange", "NULL"),
                "pre_market_change_percent": details.get("preMarketChangePercent", "NULL"),
                "regular_market_time": details.get("regularMarketTime", "NULL"),
                "regular_market_price": details.get("regularMarketPrice", "NULL"),
                "regular_market_change": details.get("regularMarketChange", "NULL"),
                "regular_market_change_percent": details.get("regularMarketChangePercent", "NULL"),
                "post_market_time": details.get("postMarketTime", "NULL"),
                "post_market_price": details.get("postMarketPrice", "NULL"),
                "post_market_change": details.get("postMarketChange", "NULL"),
                "post_market_change_percent": details.get("postMarketChangePercent", "NULL"),
            }
            for symbol, details in prices.items()
        }

        # Convert the dictionary to a Pandas DataFrame
        market_panda_df = pd.DataFrame.from_dict(market_data, orient="index")
        market_panda_df.index.name = "symbol"  # Set the index name to "symbol"
        market_panda_df = market_panda_df.reset_index()
        return market_panda_df

    except Exception as e:
        # Handle exceptions and print error message
        print(f"An error occurred: {e}")
        traceback.print_exc()
        return pd.DataFrame()  # Return an empty DataFrame on error


def get_raw_market_quote_consolidated(group_symbol_list: list) -> pd.DataFrame:
    """
    Fetch and consolidate EOD records for grouped symbols.

    Args:
        group_date_symbol_list (list): List of tuples containing (group_id, group_start_date, symbol).

    Returns:
        pd.DataFrame: Consolidated DataFrame of EOD records.
    """
    try:
        largest_group_id = max(group_symbol_list, key=lambda x: x[0])[0]
        grouped_symbols = defaultdict(list)
        
        # Group symbols by (group_id, group_start_date)
        for group_id, symbol in group_symbol_list:
            grouped_symbols[(group_id)].append(symbol)
        
        stacked_group_panda_dfs = []
        for group_id, group_symbols in grouped_symbols.items():
            print(f"Processing group {group_id}/{largest_group_id} with {len(group_symbols)} symbols...")
            group_panda_df = get_raw_market_quote(group_symbols)
            print(f"Retrieved {len(group_panda_df)} records for group {group_id}/{largest_group_id}.")
            
            stacked_group_panda_dfs.append(group_panda_df)
            sleep_time = random.randint(1, 5)
            print(f"Sleeping for {sleep_time} seconds...")
            time.sleep(sleep_time)
        
        # Combine all DataFrames into one
        consolidated_market_panda_df = pd.concat(stacked_group_panda_dfs, ignore_index=True)
        return consolidated_market_panda_df
    
    except Exception as e:
        # Handle exceptions and return an empty DataFrame
        print(f"An error occurred: {e}")
        traceback.print_exc()
        return pd.DataFrame(columns=["symbol", "exchange_name", "current_market_state", "pre_market_time", "pre_market_price", "pre_market_change", \
                                     "pre_market_change_percent", "regular_market_time", "regular_market_price", \
                                     "regular_market_change", "regular_market_change_percent", "post_market_time", \
                                     "post_market_price", "post_market_change", "post_market_change_percent"])









        