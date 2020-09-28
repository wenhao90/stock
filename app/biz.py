# 业务操作

import clazz.entity as entity
import data.index as index
import data.stock as stock
import data.mongo as mongo
from app import join_quant as jq
from clazz.entity import Stock


def init():
    jq.login()

    count = mongo.get_count("stock")
    print("表数据条数为：", count)

    if count is 0:
        stock_init("000902.XSHG")

    print(stock.get_stock_by_code("000001.XSHE"))


# 中证流通 000902.XSHG
# 中证 800 000906.XSHG
def stock_init(index_str):
    index_list = index.get_stock_by_index(index_str)

    length = len(index_list)
    stock_list = [length]

    for i in range(length):
        code = index_list[i]
        stock: Stock = entity.Stock(code, "")

        stock_list.append(stock.to_json())

    print(stock_list)
    mongo.insert_many("stock", stock_list)
    print("获取条数：", length)


init()
