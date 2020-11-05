# 股票相关的操作
import jqdatasdk as sdk
from jqdatasdk import finance
from jqdatasdk import query
from jqdatasdk import macro
from app import join_quant as jq

import mysql


# JQData证券代码标准格式
# 上海证券交易所   .XSHG
# 深圳证券交易所   .XSHE


def init_security():
    jq.login()


def init_stock():
    jq.login()

    # 查询财务数据
    data = sdk.get_fundamentals(sdk.query(sdk.valuation), '2020-10-30')

    sql = "insert into security(code, market_cap, circulating_market_cap, pe_ratio, pb_ratio, ps_ratio, pcf_ratio, type) values (%s, %s, %s, %s, %s, %s, %s, %s)"
    args = []
    for i in data.index:
        arg = (data.iloc[i]['code'], float(data.iloc[i]['market_cap']), float(data.iloc[i]['circulating_market_cap']),
               float(data.iloc[i]['pe_ratio']), float(data.iloc[i]['pb_ratio']), float(data.iloc[i]['ps_ratio']),
               float(data.iloc[i]['pcf_ratio']),
               'stock')
        print (arg)
        # args.append(arg)
        mysql.insert_one(sql, arg)
        return


init_stock()

# jq.login()

# 获取所有标的信息
# types
# stock(股票)
# index(指数)
# etf(ETF基金)
# data = sdk.get_all_securities(types=['stock'], date='2020-10-30')

# 上市公司基本信息
# data = finance.run_query(
#     query(finance.STK_COMPANY_INFO).filter(finance.STK_COMPANY_INFO.code == '000001.XSHE').limit(1))

# 查询财务数据
# data = sdk.get_fundamentals(sdk.query(sdk.valuation).filter(sdk.valuation.code == '000001.XSHE'), '2020-10-30')

# 获取融资标的列表
# data = sdk.get_margincash_stocks(date='2020-10-30')

# 获取融券标的列表
# data = sdk.get_marginsec_stocks(date='2020-10-30')

# 在策略中获取个股未来的解禁情况
# data = sdk.get_locked_shares(stock_list=['000001.XSHE'], start_date='2020-10-30', forward_count=60)

# 获取行业概念成分股
# data = sdk.get_concept('000001.XSHE', '2020-10-30')

# 获取行业列表
# "sw_l1": 申万一级行业
# "sw_l2": 申万二级行业
# "sw_l3": 申万三级行业
# "jq_l1": 聚宽一级行业
# "jq_l2": 聚宽二级行业
# "zjw": 证监会行业
# data = sdk.get_industries(name='sw_l1', date='2020-10-30')

# 获取概念列表
# data = sdk.get_concepts()

# 查询股票所属行业
# data = sdk.get_industry('600519.XSHG', date='2020-10-30')

# 获取行业指数数据
# data = finance.run_query(sdk.query(finance.SW1_DAILY_PRICE).filter(finance.SW1_DAILY_PRICE.code == '801010').limit(1))

# 获取行情数据
# data = sdk.get_price('600519.XSHG', start_date='2020-10-30', end_date='2020-10-30', frequency='daily', fq='pre')

# 获取融资融券信息
# data = sdk.get_mtss('000001.XSHE', '2020-10-30', '2020-10-30')

# 获取龙虎榜数据获取龙虎榜数据
# data = sdk.get_billboard_list(stock_list=None, end_date='2020-10-30', count=1)

# 沪深市场每日成交概况
# 322002	上海A股
# 322005	深市主板
# data = finance.run_query(sdk.query(finance.STK_EXCHANGE_TRADE_INFO).filter(
#     finance.STK_EXCHANGE_TRADE_INFO.exchange_code == '322002').limit(1))

# 获取融资融券汇总数据
# data = finance.run_query(
#     sdk.query(finance.STK_MT_TOTAL).filter(finance.STK_MT_TOTAL.date == '2020-10-30').limit(2))


# 财务指标数据(一季度)
# data = sdk.get_fundamentals(sdk.query(sdk.indicator).filter(sdk.valuation.code == '000001.XSHE'), '2020-10-30')

# 合并现金流量表
# data = finance.run_query(sdk.query(finance.STK_CASHFLOW_STATEMENT).filter(finance.STK_CASHFLOW_STATEMENT.code=='000001.XSHE').limit(1))

# 合并资产负债表
# data = finance.run_query(
#     sdk.query(finance.STK_BALANCE_SHEET).filter(finance.STK_BALANCE_SHEET.code == '000001.XSHE').limit(1))

# 十大流通股东
# data = finance.run_query(query(finance.STK_SHAREHOLDER_FLOATING_TOP10).filter(finance.STK_SHAREHOLDER_FLOATING_TOP10.code=='000001.XSHE').limit(10))

# 股东股份质押
# data = finance.run_query(
#     query(finance.STK_SHARES_PLEDGE).filter(finance.STK_SHARES_PLEDGE.code == '000001.XSHE').limit(10))

# 大股东增减持
# data = finance.run_query(query(finance.STK_SHAREHOLDERS_SHARE_CHANGE).filter(finance.STK_SHAREHOLDERS_SHARE_CHANGE.code=='000001.XSHE').limit(10))

# 宏观经济
# data = macro.run_query(query(macro.table_name).filter(macro.table_name.indicator==value).limit(n))

# print(data)
