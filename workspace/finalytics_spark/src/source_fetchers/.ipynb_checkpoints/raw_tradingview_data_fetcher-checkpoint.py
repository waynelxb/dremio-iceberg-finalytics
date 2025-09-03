# TradingView provides data of the components of indexes
# This fetcher is to fetch the data of SPY components from Trading View URLs
# The input URLs will be put in a list

import sys
import traceback
import requests
import json
import traceback
from bs4 import BeautifulSoup
import re
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, FloatType, DoubleType
from pyspark.sql.functions import col
from datetime import datetime
from collections import defaultdict
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class RawTradingViewDataFetcher:
    def __init__(self, url_list):
        self.url_list = url_list
        self.current_datetime_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    def scrape_tradingview_data_from_url(self, url):               
        try:     
            # Scrape data from URL
            # url = "https://www.tradingview.com/symbols/SP-S5CONS/components/"  
            timeout_seconds = 5
            req = requests.get(url, timeout=timeout_seconds)      
            html = req.text
            soup = BeautifulSoup(html, 'html.parser')
            
            # Get field name
            field_list=[]
            for tag_td in soup.select("th[class*='cell-']"):
                field = tag_td.text.replace(".", "").replace(" ", "_") \
                    .replace("_", "").replace("%", "Pct").replace("/", "").replace("(", "") \
                    .replace(")", "").replace("*", "").replace('\xa0', ' ')        
                field_list.append(field)                
            # Add the field ImportDatetime
            field_list.append("ImportDatetime")
            
            # Build raw data schema
            record_schema = StructType([
                StructField(field, eval('StringType')(), True)
                for field in field_list
            ])
            # print(record_schema)
            
            # Get Records        
            # Each record is a tuple, record_tuple_list is a collection of records
            record_tuple_list=[]
            for tag_tr in soup.select("tr[class*=' listRow']"):
                tag_a = tag_tr.find('a', attrs={'class': re.compile('^tickerName.*')})
                ticker = tag_a.text
                record = ""
                record_list=[ticker]
                for tag_td in tag_tr.select("td[class*='cell-']"):
                    if tag_td.text.find(ticker) < 0:
                        record_list.append(tag_td.text.replace("\u202f", ""))
                record_list.append(self.current_datetime_str)
                # Tuple is immutable in Python, so we cannot append an element to a tuple. 
                # This is why we need to build up the list first then convert it to a tuple.
                record_tuple= tuple(record_list)
                record_tuple_list.append(record_tuple)
            return record_tuple_list        
            # # Create dataframe
            # df = spark.createDataFrame(record_tuple_list, schema)
            # df.select("symbol", "Sector","ImportDatetime").show()   
        
        except:
            exc_type, exc_value, exc_traceback = sys.exc_info()
            exceptMessage = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
            message = "Error(-1): The data cannot be downloaded. <Except Message: " + exceptMessage + "> <Quote URL: "
            print(message)

    # def concatenate_tradingview_raw_data(self):
    #     all_record_tuple_list=[]
    #     for url in self.url_list:
    #         # print(url)
    #         all_record_tuple_list.extend(self.scrape_tradingview_data_from_url(url))
    #     return all_record_tuple_list


# url_list = ["https://www.tradingview.com/symbols/SP-S5CONS/components/","https://www.tradingview.com/symbols/SP-S5MATR/components/"]   

# MyRawTradingViewDataFetcher=RawTradingViewDataFetcher(url_list)
# x=MyRawTradingViewDataFetcher.concatenate_tradingview_raw_data()
# print(x)



        

        