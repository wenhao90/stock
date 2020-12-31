from data import mysql_util as my
from data import math

import numpy as np


# 初始化 sma
def init_sma(limit, ma_num):
    stocks_sql = "select code from security"
    stock_codes = my.select_all(stocks_sql, ())

    for stock_code in stock_codes:
        code = stock_code['code']

        price_query_sql = "select date, close from stock_price where code = %s order by date desc limit %s"
        price_data = my.select_all(price_query_sql, (code, limit + ma_num))

        price_list = []
        for price in price_data:
            price_value = price['close']
            price_list.append(price_value)

        price_list = np.array(price_list)
        sma_list = []
        for i in range(len(price_data)):
            sma_value = math.get_sma(price_list[i:20 + i])

            price = price_data[i]
            sma = (sma_value, code, price['date'])
            sma_list.append(sma)

            if i >= limit - 1:
                break

        print(sma_list)
        update_sql = "update stock_price set sma_20 = %s where code = %s and date = %s"
        my.update_many(update_sql, sma_list)



