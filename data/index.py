# 指数相关的操作
import jqdatasdk as jq


# 根据指数获取基本股票
def get_stock_by_index(index_str):
    stock_list = jq.get_index_stocks(index_str)
    return stock_list
