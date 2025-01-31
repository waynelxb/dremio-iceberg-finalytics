from yahooquery import Ticker
import pandas as pd
from datetime import date, timedelta
from collections import defaultdict
import random
import time
import warnings
import traceback
import numpy as np

warnings.filterwarnings("ignore", category=FutureWarning, module="yahooquery")


class RawYahooDataCollector:
    def __init__(self, record_type, grouped_symbol_list):
        self.record_type = record_type
        self.grouped_symbol_list = grouped_symbol_list

        if not grouped_symbol_list or not isinstance(self.grouped_symbol_list, list):
            raise ValueError("grouped_symbol_list must be a non-empty list")
            
        if self.record_type not in ("eod_quote", "market_quote"):
            raise ValueError(f"Invalid value: {record_type}. Expected one of ('eod_quote', 'market_quote').")

        group_field_count = len(self.grouped_symbol_list[0])

        if self.record_type == "eod_quote" and group_field_count != 3:
            raise ValueError("grouped_symbol_list for eod quotes must have 3 elements: group_id, start_date, and symbol")
        elif self.record_type == "market_quote" and group_field_count != 2:
            raise ValueError("grouped_symbol_list for market quotes must have 2 elements: group_id and symbol")

    def get_raw_eod_quotes(self, symbol_list: list, start_date: date) -> pd.DataFrame:
        try:
            end_date = date.today() + timedelta(days=1)

            data = Ticker(symbol_list)
            hist_data = data.history(start=start_date, end=end_date, interval="1d").reset_index()

            columns_to_select = ["date", "symbol", "open", "high", "low", "close", "volume"]
            hist_data = hist_data[columns_to_select]
            hist_data["date"] = pd.to_datetime(hist_data["date"].astype(str).str.slice(0, 10))

            return hist_data

        except Exception as e:
            print(f"An error occurred: {e}, {symbol_list}, {start_date}")
            traceback.print_exc()
            return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])

    def get_raw_eod_quotes_consolidated(self) -> pd.DataFrame:
        try:
            if not self.grouped_symbol_list:
                return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])

            largest_group_id = max(self.grouped_symbol_list, key=lambda x: x[0])[0]
            grouped_symbols = defaultdict(list)

            for group_id, group_start_date, symbol in self.grouped_symbol_list:
                grouped_symbols[(group_id, group_start_date)].append(symbol)

            stacked_hist_group_panda_dfs = []

            for (group_id, group_start_date), group_symbols in grouped_symbols.items():
                print(f"Processing group {group_id}/{largest_group_id} with {len(group_symbols)} symbols...")
                hist_group_panda_df = self.get_raw_eod_quotes(group_symbols, group_start_date)
                print(f"Retrieved {len(hist_group_panda_df)} records for group {group_id}/{largest_group_id}.")

                stacked_hist_group_panda_dfs.append(hist_group_panda_df)

                sleep_time = random.randint(1, 5)
                print(f"Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)

            consolidated_hist_panda_df = pd.concat(stacked_hist_group_panda_dfs, ignore_index=True)
            return consolidated_hist_panda_df

        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()
            return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume"])

    def get_raw_market_quotes(self, symbol_list: list) -> pd.DataFrame:
        try:
            t = Ticker(symbol_list)
            prices = t.price

            # market_data = {
            #     symbol: {
            #         "exchange_name": details.get("exchangeName", "NULL"),
            #         "market_state": details.get("marketState", "NULL"),
            #         "pre_market_time": details.get("preMarketTime", "NULL"),
            #         "pre_market_price": details.get("preMarketPrice", "NULL"),
            #         "pre_market_change": details.get("preMarketChange", "NULL"),
            #         "pre_market_change_percent": details.get("preMarketChangePercent", "NULL"),
            #         "regular_market_time": details.get("regularMarketTime", "NULL"),
            #         "regular_market_price": details.get("regularMarketPrice", "NULL"),
            #         "regular_market_change": details.get("regularMarketChange", "NULL"),
            #         "regular_market_change_percent": details.get("regularMarketChangePercent", "NULL"),
            #         "post_market_time": details.get("postMarketTime", "NULL"),
            #         "post_market_price": details.get("postMarketPrice", "NULL"),
            #         "post_market_change": details.get("postMarketChange", "NULL"),
            #         "post_market_change_percent": details.get("postMarketChangePercent", "NULL"),
            #     }
            #     for symbol, details in prices.items()
            # }



            market_data = {
                symbol: {
                    "exchange_name": details.get("exchangeName", "NULL"),
                    "market_state": details.get("marketState", "NULL"),
                    "pre_market_time": pd.to_datetime(details.get("preMarketTime", None), unit='s', errors='coerce'),
                    "pre_market_price": float(details.get("preMarketPrice", np.nan)) if details.get("preMarketPrice") is not None else np.nan,
                    "pre_market_change": float(details.get("preMarketChange", np.nan)) if details.get("preMarketChange") is not None else np.nan,
                    "pre_market_change_percent": float(details.get("preMarketChangePercent", np.nan)) if details.get("preMarketChangePercent") is not None else np.nan,
                    "regular_market_time": pd.to_datetime(details.get("regularMarketTime", None), unit='s', errors='coerce'),
                    "regular_market_price": float(details.get("regularMarketPrice", np.nan)) if details.get("regularMarketPrice") is not None else np.nan,
                    "regular_market_change": float(details.get("regularMarketChange", np.nan)) if details.get("regularMarketChange") is not None else np.nan,
                    "regular_market_change_percent": float(details.get("regularMarketChangePercent", np.nan)) if details.get("regularMarketChangePercent") is not None else np.nan,
                    "post_market_time": pd.to_datetime(details.get("postMarketTime", None), unit='s', errors='coerce'),
                    "post_market_price": float(details.get("postMarketPrice", np.nan)) if details.get("postMarketPrice") is not None else np.nan,
                    "post_market_change": float(details.get("postMarketChange", np.nan)) if details.get("postMarketChange") is not None else np.nan,
                    "post_market_change_percent": float(details.get("postMarketChangePercent", np.nan)) if details.get("postMarketChangePercent") is not None else np.nan,
                }
                for symbol, details in prices.items()
            }
            
            market_panda_df = pd.DataFrame.from_dict(market_data, orient="index").reset_index()
            market_panda_df.rename(columns={"index": "symbol"}, inplace=True)
            return market_panda_df

        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    def get_raw_market_quotes_consolidated(self) -> pd.DataFrame:
        try:
            if not self.grouped_symbol_list:
                return pd.DataFrame(columns=["symbol", "exchange_name", "market_state", "pre_market_time", 
                                             "pre_market_price", "pre_market_change", "pre_market_change_percent", 
                                             "regular_market_time", "regular_market_price", "regular_market_change", 
                                             "regular_market_change_percent", "post_market_time", "post_market_price", 
                                             "post_market_change", "post_market_change_percent"])

            largest_group_id = max(self.grouped_symbol_list, key=lambda x: x[0])[0]
            grouped_symbols = defaultdict(list)

            for group_id, symbol in self.grouped_symbol_list:
                grouped_symbols[group_id].append(symbol)

            stacked_group_panda_dfs = []

            for group_id, group_symbols in grouped_symbols.items():
                print(f"Processing group {group_id}/{largest_group_id} with {len(group_symbols)} symbols...")
                group_panda_df = self.get_raw_market_quotes(group_symbols)
                print(f"Retrieved {len(group_panda_df)} records for group {group_id}/{largest_group_id}.")

                stacked_group_panda_dfs.append(group_panda_df)

                sleep_time = random.randint(1, 5)
                print(f"Sleeping for {sleep_time} seconds...")
                time.sleep(sleep_time)

            consolidated_market_panda_df = pd.concat(stacked_group_panda_dfs, ignore_index=True)
            return consolidated_market_panda_df

        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    def get_raw_yahoo_data(self) -> pd.DataFrame:
        if self.record_type == "eod_quote":
            return self.get_raw_eod_quotes_consolidated()
        elif self.record_type == "market_quote":
            return self.get_raw_market_quotes_consolidated()
        else:
            raise ValueError(f"Invalid record_type: {self.record_type}")
