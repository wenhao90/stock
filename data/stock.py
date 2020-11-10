# 股票相关的操作
# coding:utf-8

import jqdatasdk as sdk
from jqdatasdk import finance
from jqdatasdk import query
from jqdatasdk import macro

from app import join_quant as jq

import mysql
import math
import time
from datetime import datetime


# JQData证券代码标准格式
# 上海证券交易所   .XSHG
# 深圳证券交易所   .XSHE


# 初始化股票数据（大于100E）
def init_stock():
    jq.login()

    # 查询财务数据
    data = sdk.get_fundamentals(sdk.query(sdk.valuation), '2020-10-30')

    sql = "insert into security(code, market_cap, circulating_market_cap, pe_ratio, pb_ratio, ps_ratio, pcf_ratio, type) values (%s, %s, %s, %s, %s, %s, %s, %s)"

    args = []
    for i in data.index:
        code = data.iloc[i]['code']
        market_cap = is_nan(float(data.iloc[i]['market_cap']))

        if market_cap < 100:
            continue

        circulating_market_cap = is_nan(float(data.iloc[i]['circulating_market_cap']))
        pe_ratio = is_nan(float(data.iloc[i]['pe_ratio']))
        pb_ratio = is_nan(float(data.iloc[i]['pb_ratio']))
        ps_ratio = is_nan(float(data.iloc[i]['ps_ratio']))
        pcf_ratio = is_nan(float(data.iloc[i]['pcf_ratio']))
        arg = (code, market_cap, circulating_market_cap, pe_ratio, pb_ratio, ps_ratio, pcf_ratio, 'stock')

        print(arg)
        args.append(arg)

    mysql.insert_many(sql, args)


def is_nan(x):
    if math.isnan(x):
        return 0
    else:
        return x


# 初始化股票公司信息
def init_stock_info():
    stocks_sql = "select code from security"
    stock_codes = mysql.select_all(stocks_sql, ())

    jq.login()

    stock_infos = []
    for stock_code in stock_codes:
        code = stock_code['code']
        company_info = finance.run_query(
            query(finance.STK_COMPANY_INFO).filter(finance.STK_COMPANY_INFO.code == code).limit(1))

        company_id = int(company_info.iloc[0]['company_id'])
        full_name = company_info.iloc[0]['full_name']
        short_name = company_info.iloc[0]['short_name']
        register_location = company_info.iloc[0]['register_location']
        office_address = company_info.iloc[0]['office_address']
        register_capital = is_nan(float(company_info.iloc[0]['register_capital']))
        main_business = company_info.iloc[0]['main_business']
        business_scope = company_info.iloc[0]['business_scope']
        description = company_info.iloc[0]['description']
        province = company_info.iloc[0]['province']
        city = company_info.iloc[0]['city']
        comments = company_info.iloc[0]['comments']

        stock_info = (
            company_id, full_name, short_name, register_location, office_address, register_capital,
            main_business, business_scope, description, province, city, comments, code)

        print(stock_info)
        stock_infos.append(stock_info)

    update_stock_info_sql = "update security set company_id = %s, full_name = %s, short_name = %s, register_location = %s, office_address = %s," \
                            " register_capital = %s, main_business = %s, business_scope = %s, description = %s, province = %s, city = %s, comments = %s" \
                            " where code = %s"
    mysql.update_many(update_stock_info_sql, stock_infos)


# 初始化是否融资融券数据
def init_margincash_or_marginsec():
    jq.login()

    margincash_stocks = sdk.get_margincash_stocks(date='2020-10-30')
    print(margincash_stocks)

    update_sql = "update security set margincash =1, marginsec = 1 where code = %s"
    mysql.update_many(update_sql, margincash_stocks)


# "sw_l1": 申万一级行业
# "sw_l2": 申万二级行业
# "jq_l1": 聚宽一级行业
# "jq_l2": 聚宽二级行业
# "zjw": 证监会行业
def init_industry():
    jq.login()

    industry_list = []

    # sw_l1
    sw_l1_data = sdk.get_industries(name='sw_l1', date='2020-10-30')
    for index in sw_l1_data.index:
        name = sw_l1_data.loc[index]['name']
        industry = (index, name, 'sw_l1')
        industry_list.append(industry)

    # sw_l2
    sw_l1_data = sdk.get_industries(name='sw_l1', date='2020-10-30')
    for index in sw_l1_data.index:
        name = sw_l1_data.loc[index]['name']
        industry = (index, name, 'sw_l2')
        industry_list.append(industry)

    # jq_l1
    sw_l1_data = sdk.get_industries(name='jq_l1', date='2020-10-30')
    for index in sw_l1_data.index:
        name = sw_l1_data.loc[index]['name']
        industry = (index, name, 'jq_l1')
        industry_list.append(industry)

    # jq_l2
    sw_l1_data = sdk.get_industries(name='jq_l2', date='2020-10-30')
    for index in sw_l1_data.index:
        name = sw_l1_data.loc[index]['name']
        industry = (index, name, 'jq_l2')
        industry_list.append(industry)

    # zjw
    sw_l1_data = sdk.get_industries(name='zjw', date='2020-10-30')
    for index in sw_l1_data.index:
        name = sw_l1_data.loc[index]['name']
        industry = (index, name, 'zjw')
        industry_list.append(industry)

    print(industry_list)
    insert_sql = "insert into industry(code, name, type) values (%s, %s, %s)"
    mysql.insert_many(insert_sql, industry_list)


# 初始化股票所属行业
def init_stock_industries():
    stocks_sql = "select code from security"
    stock_codes = mysql.select_all(stocks_sql, ())

    jq.login()
    stock_industry_list = []

    for stock_code in stock_codes:
        code = stock_code['code']
        data = sdk.get_industry(code, date='2020-10-30')
        print(data)

        stock_industry_data = data[code]
        if not bool(stock_industry_data):
            continue

        industry_zjw = stock_industry_data['zjw']
        stock_industry_zjw = (code, 'zjw', industry_zjw['industry_code'], industry_zjw['industry_name'])
        stock_industry_list.append(stock_industry_zjw)

        has_sw_l1 = 'sw_l1' in stock_industry_data.keys()
        if has_sw_l1:
            industry_sw_l1 = stock_industry_data['sw_l1']
            industry_sw_l2 = stock_industry_data['sw_l2']

            stock_industry_sw_l1 = (code, 'sw_l1', industry_sw_l1['industry_code'], industry_sw_l1['industry_name'])
            stock_industry_sw_l2 = (code, 'sw_l2', industry_sw_l2['industry_code'], industry_sw_l2['industry_name'])

            stock_industry_list.append(stock_industry_sw_l1)
            stock_industry_list.append(stock_industry_sw_l2)

        has_jq_l1 = 'jq_l1' in stock_industry_data.keys()
        if has_jq_l1:
            industry_jq_l1 = stock_industry_data['jq_l1']
            stock_industry_jq_l1 = (code, 'jq_l1', industry_jq_l1['industry_code'], industry_jq_l1['industry_name'])
            stock_industry_list.append(stock_industry_jq_l1)

        has_jq_l2 = 'jq_l2' in stock_industry_data.keys()
        if has_jq_l2:
            industry_jq_l2 = stock_industry_data['jq_l2']
            stock_industry_jq_l2 = (code, 'jq_l2', industry_jq_l2['industry_code'], industry_jq_l2['industry_name'])
            stock_industry_list.append(stock_industry_jq_l2)

    insert_sql = "insert into stock_industry(code, type, industry_code, industry_name) values (%s, %s, %s, %s)"
    mysql.insert_many(insert_sql, stock_industry_list)


# 初始化股票行情
def init_stock_price():
    stocks_sql = "select code from security"
    stock_codes = mysql.select_all(stocks_sql, ())

    jq.login()

    for stock_code in stock_codes:
        code = stock_code['code']
        exist_sql = "select count(1) count from stock_price where code = %s"
        exist = mysql.select_one(exist_sql, code)

        if exist['count'] > 0:
            print('%s had init', code)
            continue

        price_data = sdk.get_price(code, start_date='2020-01-01', end_date='2020-10-30', frequency='daily', fq='pre')

        stock_price_list = []
        for index in price_data.index:
            index_price = price_data.loc[index]

            open = float(index_price['open'])
            if math.isnan(open):
                continue

            close = float(index_price['close'])
            low = float(index_price['low'])
            high = float(index_price['high'])
            volume = float(index_price['volume'])
            money = float(index_price['money'])

            date = index.strftime('%Y-%m-%d')
            stock_price = (code, date, open, close, low, high, volume, money)
            print(stock_price)
            stock_price_list.append(stock_price)

        insert_sql = "insert into stock_price(code, date, open, close, low, high, volume, money) values (%s, %s, %s, %s, %s, %s, %s, %s)"
        mysql.insert_many(insert_sql, stock_price_list)

        update_highest = "update security set highest = (select close from stock_price where code = %s order by close desc limit 1) where code = %s"
        mysql.update_one(update_highest, (code, code))

        update_lowest = "update security set lowest = (select close from stock_price where code = %s order by close asc limit 1) where code = %s"
        mysql.update_one(update_lowest, (code, code))


# 初始化概念列表
def init_concept():
    jq.login()

    concept_data = sdk.get_concepts()

    concept_list = []
    for index in concept_data.index:
        index_concept = concept_data.loc[index]

        concept = (index, index_concept['name'])
        print(concept)
        concept_list.append(concept)

    insert_sql = "insert into concept(code, name) values (%s, %s)"
    mysql.insert_many(insert_sql, concept_list)


# 初始化股票概念
def ini_stock_concept():
    stocks_sql = "select code from security"
    stock_codes = mysql.select_all(stocks_sql, ())

    jq.login()

    for stock_code in stock_codes:
        code = stock_code['code']

        stock_concept_data = sdk.get_concept(code, '2020-10-30')

        stock_concept_list = []
        stock_concept = stock_concept_data[code]['jq_concept']
        for concept in stock_concept:
            concept_data = (code, concept['concept_code'], concept['concept_name'], 1)
            print(concept_data)
            stock_concept_list.append(concept_data)

        insert_sql = "insert into stock_concept(code, concept_code, concept_name, status) values (%s, %s, %s, %s)"
        mysql.insert_many(insert_sql, stock_concept_list)


# 初始化行业指数数据
def init_index_price():
    industry_sql = "select code from industry where type = 'sw_l1'"
    industry_codes = mysql.select_all(industry_sql, ())

    jq.login()

    index_price_list = []
    for industry_code in industry_codes:
        code = industry_code['code']

        jq1_price_data = finance.run_query(
            sdk.query(finance.SW1_DAILY_PRICE).filter(finance.SW1_DAILY_PRICE.code == code).order_by(
                finance.SW1_DAILY_PRICE.date.desc()).limit(100))

        for index in jq1_price_data.index:
            index_jq1_price = jq1_price_data.iloc[index]

            name = index_jq1_price['name']
            code = index_jq1_price['code']
            date = index_jq1_price['date'].strftime('%Y-%m-%d')
            open = float(index_jq1_price['open'])
            high = float(index_jq1_price['high'])
            low = float(index_jq1_price['low'])
            close = float(index_jq1_price['close'])
            volume = float(index_jq1_price['volume'])
            money = float(index_jq1_price['money'])
            change_pct = float(index_jq1_price['change_pct'])

            jq1_price = (name, code, date, open, high, low, close, volume, money, change_pct)
            print(jq1_price)
            index_price_list.append(jq1_price)

    insert_sql = "insert into index_price(name, code, date, open, high, low, close, volume, money, change_pct) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    mysql.insert_many(insert_sql, index_price_list)


# 初始化沪深市场每日成交概况
# 322002	上海A股
# 322005	深市主板
# TODO
def init_exchange_trade():
    jq.login()

    data = finance.run_query(sdk.query(finance.STK_EXCHANGE_TRADE_INFO).filter(
        finance.STK_EXCHANGE_TRADE_INFO.exchange_code == '322002').limit(1))


# 获取融资融券汇总数据
def init_market_total():
    jq.login()

    # 沪深两市就是2条数据
    total_data = finance.run_query(
        sdk.query(finance.STK_MT_TOTAL).order_by(finance.STK_MT_TOTAL.date.desc()).limit(200))

    index_total_list = []
    for index in total_data.index:
        index_total_data = total_data.iloc[index]

        date = index_total_data['date'].strftime('%Y-%m-%d')
        exchange_code = index_total_data['exchange_code']
        fin_value = float(index_total_data['fin_value'])
        fin_buy_value = float(index_total_data['fin_buy_value'])
        sec_volume = int(index_total_data['sec_volume'])
        sec_value = float(index_total_data['sec_value'])
        sec_sell_volume = int(index_total_data['sec_sell_volume'])
        fin_sec_value = float(index_total_data['fin_sec_value'])

        index_total = (
            date, exchange_code, fin_value, fin_buy_value, sec_volume, sec_value, sec_sell_volume, fin_sec_value)
        print(index_total)
        index_total_list.append(index_total)

    insert_sql = "insert into market_toal(date, exchange_code, fin_value, fin_buy_value, sec_volume, sec_value, sec_sell_volume, fin_sec_value) " \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s)"
    mysql.insert_many(insert_sql, index_total_list)




# jq.login()


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


# 在策略中获取个股未来的解禁情况
# data = sdk.get_locked_shares(stock_list=['000001.XSHE'], start_date='2020-10-30', forward_count=60)

# 宏观经济
# data = macro.run_query(query(macro.table_name).filter(macro.table_name.indicator==value).limit(n))

# print(data)
