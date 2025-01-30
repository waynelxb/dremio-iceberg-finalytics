import sys
import traceback
import pandas
from yahooquery import Ticker


def get_attr_value(dict_info, attr):
    if attr in dict_info.keys():
        return dict_info[attr]
    else:
        return "NULL"


#symbol = sys.argv[1]

symbol = "OXSQ"

try:
    t = Ticker(symbol)
    price_info = t.price[symbol]
    # print(price_info)

    preMarketTime = get_attr_value(price_info, 'preMarketTime')
    # print(preMarketTime)
    preMarketPrice = get_attr_value(price_info, 'preMarketPrice')
    # print(preMarketPrice)
    preMarketChange = get_attr_value(price_info, 'preMarketChange')
    # print(preMarketChange)
    preMarketChangePercent = get_attr_value(price_info, 'preMarketChangePercent')
    # print(preMarketChangePercent)

    regularMarketTime = get_attr_value(price_info, 'regularMarketTime')
    # print(regularMarketTime)
    regularMarketPrice = get_attr_value(price_info, 'regularMarketPrice')
    # print(regularMarketPrice)
    regularMarketChange = get_attr_value(price_info, 'regularMarketChange')
    # print(regularMarketChange)
    regularMarketChangePercent = get_attr_value(price_info, 'regularMarketChangePercent')
    # print(regularMarketChangePercent)

    postMarketTime = get_attr_value(price_info, 'postMarketTime')
    # print(postMarketTime)
    postMarketPrice = get_attr_value(price_info, 'postMarketPrice')
    # print(postMarketPrice)
    postMarketChange = get_attr_value(price_info, 'postMarketChange')
    # print(postMarketChange)
    postMarketChangePercent = get_attr_value(price_info, 'postMarketChangePercent')
    # print(postMarketChangePercent)

    if preMarketTime == "NULL":
        preMarketTime = "1900-01-01 00:00:00"
    if regularMarketTime == "NULL":
        regularMarketTime = "1900-01-01 00:00:00"
    if postMarketTime == "NULL":
        postMarketTime = "1900-01-01 00:00:00"

    if preMarketTime > regularMarketTime and preMarketTime > postMarketTime:
        last_updated_time = preMarketTime
        last_value = preMarketPrice
        change_point = preMarketChange
        change_percent = preMarketChangePercent
        market_hour_status ="pre"
    elif postMarketTime > regularMarketTime and postMarketTime > preMarketTime:
        last_updated_time = postMarketTime
        last_value = postMarketPrice
        change_point = postMarketChange
        change_percent = postMarketChangePercent
        market_hour_status ="post"
    elif regularMarketTime > preMarketTime and regularMarketTime > postMarketTime:
        last_updated_time = regularMarketTime
        last_value = regularMarketPrice
        change_point = regularMarketChange
        change_percent = regularMarketChangePercent
        market_hour_status ="reg"
    # print(last_updated_time)
    # print(last_value)
    # print(change_point)
    # print(change_percent)

    market = "NULL"
    # change_percent = round(change_point / previous_close * 100, 2)

    record = market + "|" + market_hour_status + "|" + symbol + "|" + str(last_value) + "|" + str(
        change_point) + "|" + str(change_percent) + "|" + last_updated_time

    record = "'" + record.replace("|", "'|'") + "'"
    record = record.replace("'NULL'", "NULL").replace("'-'", "NULL")

    print(record)
except IndexError as e:
    print(f'Error(-3): Symbol {symbol}: {e}. It may be delisted.')  # print the ticker and the error


except Exception as err:
    exc_type, exc_value, exc_traceback = sys.exc_info()
    exceptMessage = repr(traceback.format_exception(exc_type, exc_value, exc_traceback))
    message = "Error(-2): Performance data cannot be scraped from YFinance. <Symbol: " + symbol + "> <Exception: " + exceptMessage + ">"
    print(message)
