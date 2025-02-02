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
            
        if self.record_type not in ("eod_quote", "market_quote", "profile"):
            raise ValueError(f"Invalid value: {record_type}. Expected one of ('eod_quote', 'market_quote', 'profile').")

        group_field_count = len(self.grouped_symbol_list[0])

        if self.record_type == "eod_quote" and group_field_count != 3:
            raise ValueError("grouped_symbol_list for eod quotes must have 3 elements: group_id, start_date, and symbol")
        elif self.record_type in ("market_quote", "profile") and group_field_count != 2:
            raise ValueError("grouped_symbol_list for market market_quote, key_stats must have 2 elements: group_id and symbol")

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



    def get_raw_market_quotes(self, symbol_list: list) -> pd.DataFrame:
        try:
            t = Ticker(symbol_list)
            prices = t.price

            market_data = {
                symbol: {
                    "exchange_name": details.get("exchangeName", None),
                    "market_state": details.get("marketState", None),
                    "pre_market_time": details.get("preMarketTime", None),
                    "pre_market_price": details.get("preMarketPrice", None),
                    "pre_market_change": details.get("preMarketChange", None),
                    "pre_market_change_percent": details.get("preMarketChangePercent", None),
                    "regular_market_time": details.get("regularMarketTime", None),
                    "regular_market_price": details.get("regularMarketPrice", None),
                    "regular_market_change": details.get("regularMarketChange", None),
                    "regular_market_change_percent": details.get("regularMarketChangePercent", None),
                    "post_market_time": details.get("postMarketTime", None),
                    "post_market_price": details.get("postMarketPrice", None),
                    "post_market_change": details.get("postMarketChange", None),
                    "post_market_change_percent": details.get("postMarketChangePercent", None),
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
            

    def get_raw_profile(symbol_list: list) -> pd.DataFrame:
        """
        Fetch both asset profile and key statistics data for a list of symbols,
        and combine them into a single record per symbol.
    
        Args:
            symbol_list (list): List of stock symbols.
    
        Returns:
            pd.DataFrame: DataFrame containing combined asset profile and key statistics data.
        """
        try:
            # Fetch data using yahooquery
            t = Ticker(symbol_list)
    
            # Fetch asset profile
            asset_profile = t.asset_profile
    
            # Fetch key statistics
            key_stats = t.key_stats
    
            # Combine data into a single dictionary
            combined_data = {}
            for symbol in symbol_list:
                # Get asset profile data for the symbol
                profile_data = asset_profile.get(symbol, {})
                # Get key statistics data for the symbol
                stats_data = key_stats.get(symbol, {})
    
                # Combine both datasets into a single record
                combined_data[symbol] = {
                    # Asset Profile Fields
                    "industry": profile_data.get("industry", "NULL"),
                    "sector": profile_data.get("sector", "NULL"),
                    # Key Statistics Fields
                    "enterprise_value": stats_data.get("enterpriseValue", "NULL"),
                    "forward_pe": stats_data.get("forwardPE", "NULL"),
                    "profit_margins": stats_data.get("profitMargins", "NULL"),
                    "float_shares": stats_data.get("floatShares", "NULL"),
                    "shares_outstanding": stats_data.get("sharesOutstanding", "NULL"),
                    "shares_short": stats_data.get("sharesShort", "NULL"),
                    "shares_short_prior_month": stats_data.get("sharesShortPriorMonth", "NULL"),
                    "short_ratio": stats_data.get("shortRatio", "NULL"),
                    "short_percent_of_float": stats_data.get("shortPercentOfFloat", "NULL"),
                    "beta": stats_data.get("beta", "NULL"),
                    "book_value": stats_data.get("bookValue", "NULL"),
                    "price_to_book": stats_data.get("priceToBook", "NULL"),
                    "last_fiscal_year_end": stats_data.get("lastFiscalYearEnd", "NULL"),
                    "next_fiscal_year_end": stats_data.get("nextFiscalYearEnd", "NULL"),
                    "most_recent_quarter": stats_data.get("mostRecentQuarter", "NULL"),
                    "earnings_quarterly_growth": stats_data.get("earningsQuarterlyGrowth", "NULL"),
                    "net_income_to_common": stats_data.get("netIncomeToCommon", "NULL"),
                    "trailing_eps": stats_data.get("trailingEps", "NULL"),
                    "forward_eps": stats_data.get("forwardEps", "NULL"),
                    "peg_ratio": stats_data.get("pegRatio", "NULL"),
                    "last_split_factor": stats_data.get("lastSplitFactor", "NULL"),
                    "last_split_date": stats_data.get("lastSplitDate", "NULL"),
                }
    
            # Convert the dictionary to a Pandas DataFrame
            combined_df = pd.DataFrame.from_dict(combined_data, orient="index").reset_index()
            combined_df.rename(columns={"index": "symbol"}, inplace=True)
            return combined_df
    
        except Exception as e:
            # Handle exceptions and print error message
            print(f"An error occurred: {e}")
            traceback.print_exc()
            return pd.DataFrame()  # Return an empty DataFrame on error


    def get_raw_yahoo_data(self) -> pd.DataFrame:
        try:
            if not self.grouped_symbol_list:
                return pd.DataFrame()
                # return pd.DataFrame(columns=["symbol","enterprise_value","forward_pe","profit_margins"
                #                              ,"float_shares","shares_outstanding"
                #                              ,"shares_short","shares_short_prior_month","short_ratio"
                #                              ,"short_percent_of_float","beta","book_value","price_to_book","last_fiscal_year_end"
                #                              ,"next_fiscal_year_end","most_recent_quarter","earnings_quarterly_growth"
                #                              ,"net_income_to_common","trailing_eps","forward_eps","peg_ratio"
                #                              ,"last_split_factor","last_split_date"])
    
    
            largest_group_id = max(self.grouped_symbol_list, key=lambda x: x[0])[0]
            grouped_symbols = defaultdict(list)
            sleep_time = random.randint(1, 5)
    
            if self.record_type == "eod_quote":
                for group_id, group_start_date, symbol in self.grouped_symbol_list:
                    grouped_symbols[(group_id, group_start_date)].append(symbol)
    
                stacked_group_panda_dfs = []
                for (group_id, group_start_date), group_symbols in grouped_symbols.items():
                    print(f"Processing group {group_id}/{largest_group_id} with {len(group_symbols)} symbols...")
                    group_panda_df = self.get_raw_eod_quotes(group_symbols, group_start_date)
                    print(f"Retrieved {len(group_panda_df)} records for group {group_id}/{largest_group_id}.")
    
                    stacked_group_panda_dfs.append(group_panda_df)                
                    print(f"Sleeping for {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    
            elif self.record_type in ("market_quote", "profile"):
                for group_id, symbol in self.grouped_symbol_list:
                    grouped_symbols[group_id].append(symbol)  
                    
                stacked_group_panda_dfs = []
                for group_id, group_symbols in grouped_symbols.items():
                    print(f"Processing group {group_id}/{largest_group_id} with {len(group_symbols)} symbols...")
                    if self.record_type == "profile":
                        group_panda_df = self.get_raw_profile(group_symbols)
                    elif self.record_type == "market_quote":
                        group_panda_df = self.get_raw_market_quotes(group_symbols)                
                    print(f"Retrieved {len(group_panda_df)} records for group {group_id}/{largest_group_id}.")
        
                    stacked_group_panda_dfs.append(group_panda_df)    
                    print(f"Sleeping for {sleep_time} seconds...")
                    time.sleep(sleep_time)
    
            consolidated_panda_df = pd.concat(stacked_group_panda_dfs, ignore_index=True)
            
            return consolidated_panda_df
    
        except Exception as e:
            print(f"An error occurred: {e}")
            traceback.print_exc()
            return pd.DataFrame()

    

