# 同步股票相关的数据
# coding:utf-8

import jqdatasdk as sdk

from app import join_quant as jq

from data import mysql_util as my
from data import stock
from data import stock_sma as ma

import time
import math
import pandas as pd
from datetime import datetime


def is_nan(x):
    if math.isnan(x):
        return 0
    else:
        return x


def is_none(x):
    if x is None:
        return 0
    else:
        return x


# 获取龙虎榜数据
def get_billboard(start_date=None, end_date=None, count=None):
    jq.login()

    billboard_data = sdk.get_billboard_list(stock_list=None, start_date=start_date, end_date=end_date, count=count)

    billboard_list = []
    for index in billboard_data.index:
        index_billboard_data = billboard_data.iloc[index]

        code = index_billboard_data['code']
        date = index_billboard_data['day'].strftime('%Y-%m-%d')
        direction = index_billboard_data['direction']
        abnormal_code = int(index_billboard_data['abnormal_code'])
        abnormal_name = index_billboard_data['abnormal_name']
        sales_depart_name = index_billboard_data['sales_depart_name']
        rank = int(index_billboard_data['rank'])
        buy_value = is_nan(float(index_billboard_data['buy_value']))
        buy_rate = is_nan(float(index_billboard_data['buy_rate']))
        sell_value = is_nan(float(index_billboard_data['sell_value']))
        sell_rate = is_nan(float(index_billboard_data['sell_rate']))
        net_value = is_nan(float(index_billboard_data['net_value']))
        amount = is_nan(float(index_billboard_data['amount']))

        index_billboard = (
            code, date, direction, abnormal_code, abnormal_name, sales_depart_name, rank, buy_value, buy_rate,
            sell_value, sell_rate, net_value, amount)
        print(index_billboard)
        billboard_list.append(index_billboard)

    insert_sql = "insert into billboard(code, date, direction, abnormal_code, abnormal_name, sales_depart_name, `rank`, buy_value, buy_rate, sell_value, sell_rate, net_value, amount) " \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, billboard_list)


# 同步股票行情
def get_stock_price(start_date, end_date):
    stock.init_stock_price(start_date, end_date)


# 同步行业行情
def get_index_price(limit):
    stock.init_index_price(limit)


# 同步融资融券汇总数据
def get_market_total(limit):
    stock.init_market_total(limit)


# 同步融资融券汇总数据
def get_sma_20(limit):
    ma.init_sma_20(limit)



