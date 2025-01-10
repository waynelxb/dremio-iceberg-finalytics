from yahooquery import Ticker
from datetime import datetime, date
import pandas as pd
import warnings
import yfinance as yf

warnings.filterwarnings("ignore", category=FutureWarning, module="yahooquery")

def get_raw_yahooquery(symbol_list, start_date, import_time):
    try:
        # Attempt to fetch data from Yahoo Finance
        data = Ticker(symbol_list)
        current_date = date.today()
       
        # Fetch historical data
        hist_data = data.history(start=start_date, end=current_date, interval="1d")
        # print(hist_data)
        
        # Reshape the data to include a 'Ticker' column
        # hist_data.reset_index(inplace=True)  # Reset index to make Ticker and Date columns
        hist_data = hist_data.reset_index()

        # # Select only required columns
        columns_to_select = ["date", "symbol", "open", "high", "low", "close", "volume"]
        hist_data = hist_data[columns_to_select]
        hist_data["date"] = hist_data["date"].astype(str).str.slice(0, 10)
        hist_data["date"] = pd.to_datetime(hist_data["date"])
        
        # Add a new field for the current datetime
        hist_data["import_time"] = pd.to_datetime(import_time).tz_localize(None)

        # Return the processed data
        return hist_data
    
    except Exception as e:
        # Handle any exception that occurred during the execution
        print(f"An error occurred: {e}")
        
        # Return an empty DataFrame with the same columns as the expected result
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "import_time"])  


def get_raw_yfinance(symbol_list, start_date, import_time):
    try:
        current_date = date.today()
        # Fetch historical data
        hist_data = yf.download(
            tickers=symbol_list,
            start=start_date,
            end=current_date,
            interval="1d",
            group_by="ticker",  # Keep data grouped by ticker if multiple symbols are provided
            auto_adjust=False,  # Keep original values without adjustment
        )
        
        # Check if the data needs reshaping for multiple tickers
        hist_data = hist_data.stack(level=0, future_stack=True).reset_index()   

        hist_data.rename(columns={"Date": "date", "Ticker":"symbol", "Open": "open", "High": "high", 
                                  "Low": "low", "Close": "close", "Volume": "volume"}, inplace=True)
        

        # hist_data.columns = [col.lower() for col in hist_data.columns]
       
        # Select only required columns
        columns_to_select = ["date", "symbol", "open", "high", "low", "close", "volume"]
        hist_data = hist_data[columns_to_select]
        
        # Ensure the date is in datetime format
        hist_data["date"] = hist_data["date"].astype(str).str.slice(0, 10)
        hist_data["date"] = pd.to_datetime(hist_data["date"])
        
        # Add an import_time column
        hist_data["import_time"] = pd.to_datetime(import_time)
        # print(hist_data)
        # # Return the processed data
        return hist_data
    
    except Exception as e:
        # Handle any exceptions
        print(f"An error occurred: {e}")        
        # Return an empty DataFrame with the same columns as the expected result
        return pd.DataFrame(columns=["date", "symbol", "open", "high", "low", "close", "volume", "import_time"])