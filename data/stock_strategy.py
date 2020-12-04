# 根据数据处理一些策略
from data import mysql_util as my

import pandas as pd


# 市场宽度(统计每个行业中，股票收盘价大于20日移动平均线的比值)
def strategy_market_width(industry_type='sw_l1', start_date=None):
    width_sql = "select industry.industry_name, industry.industry_code, price.date, count(1) count, " \
                "count(if(price.close > price.sma_20, 1, null)) success " \
                "from stock_price price left join stock_industry industry on price.code = industry.code " \
                "where industry.type = %s and price.date >= (select price_1.date from stock_price price_1 order by price_1.date desc limit 1) " \
                "group by industry.industry_name, industry.industry_code, price.date " \
                "order by price.date desc"
    width_data = my.select_all(width_sql, (industry_type))

    width_list = []
    for index_width in width_data:
        industry_name = index_width['industry_name']
        industry_code = index_width['industry_code']
        date = index_width['date']
        count = index_width['count']
        success = index_width['success']
        success_range = round(success / count, 4) * 100

        width = (industry_type, industry_name, industry_code, date, count, success, success_range)
        print(width)
        width_list.append(width)

    insert_sql = "insert into width(industry_type, industry_name, industry_code, date, count, success, `range`) " \
                 "values (%s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, width_list)


# 处于低值区的股票(range 越大表示越接近)
def strategy_low_value():
    price_sql = "select stock.code, stock.short_name,price.date,price.close,stock.highest,stock.lowest " \
                "from stock_price price left join security stock on price.code = stock.code " \
                "where price.date = (select price_1.date from stock_price price_1 order by price_1.date desc limit 1)"
    price_data = my.select_all(price_sql, ())

    low_value_list = []
    for index_price in price_data:
        close = float(index_price['close'])
        highest = float(index_price['highest'])
        lowest = float(index_price['lowest'])
        print("close: %s, highest: %s, lowest: %s", close, highest, lowest)

        low_range = _get_range(close, highest, lowest)
        if low_range > 85:
            code = index_price['code']
            short_name = index_price['short_name']
            date = index_price['date']

            low_value = (code, short_name, date, '低值', close, highest, lowest, low_range)
            low_value_list.append(low_value)

    print(low_value_list)
    insert_sql = "insert into strategy(code, company_name, date, type , price, highest, lowest, `range`)" \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, low_value_list)


# 缺口: 今天的开盘价和收盘价均大于或小于昨天的收盘价
def strategy_gap():
    stocks_sql = "select code,short_name from security"
    stock_codes = my.select_all(stocks_sql, ())

    gap_list = []
    for stock_code in stock_codes:
        code = stock_code['code']

        price_sql = "select date,close,low,high from stock_price where code = %s order by date desc limit 2"
        price_data = my.select_all(price_sql, code)

        now_low = float(price_data[0]['low'])
        now_high = float(price_data[0]['high'])

        yesterday_low = float(price_data[1]['low'])
        yesterday_high = float(price_data[1]['high'])

        if now_low > yesterday_high or yesterday_low > now_high:
            short_name = stock_code['short_name']
            now_date = price_data[0]['date']
            now_close = float(price_data[0]['close'])

            now_range = _get_range(now_close, now_high, now_low)
            gap_value = (code, short_name, now_date, '缺口', now_close, now_high, now_low, now_range)

            print(gap_value)
            gap_list.append(gap_value)

    insert_sql = "insert into strategy(code, company_name, date, type , price, highest, lowest, `range`)" \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, gap_list)


def strategy_volume_large(limit=5):
    stocks_sql = "select code,short_name,highest,lowest from security"
    stocks = my.select_all(stocks_sql, ())

    large_list = []
    for index_stock in stocks:
        code = index_stock['code']

        price_sql = "select date, volume,close from stock_price where code = %s order by date desc limit %s"
        large_data = my.select_all(price_sql, (code, limit + 1))

        total = 0
        for index in range(len(large_data)):
            if index == 0:
                continue
            total += float(large_data[index]['volume'])
        avg = round(total / limit, 4)

        now_volume = float(large_data[0]['volume'])
        if now_volume > avg * 3:
            short_name = index_stock['short_name']
            now_date = large_data[0]['date']
            now_close = float(large_data[0]['close'])

            highest = float(index_stock['highest'])
            lowest = float(index_stock['lowest'])

            now_range = _get_range(now_close, highest, lowest)
            large_value = (code, short_name, now_date, '大量', now_close, highest, lowest, now_range)
            print(large_value)
            large_list.append(large_value)

    insert_sql = "insert into strategy(code, company_name, date, type , price, highest, lowest, `range`)" \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, large_list)


# 均线相关策略：破线 + 拐头
def strategy_ma():
    stocks_sql = "select code,short_name,highest,lowest from security"
    stocks = my.select_all(stocks_sql, ())

    ma_list = []
    for index_stock in stocks:
        code = index_stock['code']

        price_sql = "select date, sma_20, close from stock_price where code = %s order by date desc limit 2"
        ma_data = my.select_all(price_sql, code)

        now_close = float(ma_data[0]['close'])
        now_ma_20 = float(ma_data[0]['sma_20'])

        yesterday_close = float(ma_data[1]['close'])
        yesterday_ma_20 = float(ma_data[1]['sma_20'])

        if yesterday_close > yesterday_ma_20:
            continue

        strategy_type = None
        if now_close > now_ma_20:
            strategy_type = '破线'

        if now_ma_20 > yesterday_ma_20:
            if strategy_type is None:
                strategy_type = '拐头'
            else:
                strategy_type += '拐头'

        if strategy_type is None:
            continue

        short_name = index_stock['short_name']
        now_date = ma_data[0]['date']
        now_close = float(ma_data[0]['close'])

        highest = float(index_stock['highest'])
        lowest = float(index_stock['lowest'])

        now_range = _get_range(now_close, highest, lowest)
        ma_value = (code, short_name, now_date, strategy_type, now_close, highest, lowest, now_range)
        print(ma_value)
        ma_list.append(ma_value)

    insert_sql = "insert into strategy(code, company_name, date, type , price, highest, lowest, `range`)" \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, ma_list)

    # 每天3点，更新完股票价格之后


def strategy_slump():
    stocks_sql = "select code,short_name,highest,lowest from security"
    stocks = my.select_all(stocks_sql, ())

    slump_list = []
    for index_stock in stocks:
        code = index_stock['code']

        price_sql = "select date, close from stock_price where code = %s order by date desc limit 6"
        slump_data = my.select_all(price_sql, code)

        now_close = float(slump_data[0]['close'])

        # 两天前的收盘价
        pre_2_close = float(slump_data[2]['close'])
        pre_5_close = float(slump_data[5]['close'])

        # 跌幅
        range_2 = round((pre_2_close - now_close) / pre_2_close, 4)
        range_5 = round((pre_5_close - now_close) / pre_5_close, 4)

        if range_2 > 0.15:
            short_name = index_stock['short_name']
            now_date = slump_data[0]['date']

            highest = float(index_stock['highest'])
            lowest = float(index_stock['lowest'])

            now_range_2 = _get_range(now_close, highest, lowest)
            slump_value = (code, short_name, now_date, '急跌: 2日', now_close, highest, lowest, now_range_2)
            print(slump_value)
            slump_list.append(slump_value)

        if range_5 > 0.3:
            short_name = index_stock['short_name']
            now_date = slump_data[0]['date']

            now_range_5 = _get_range(now_close, highest, lowest)
            slump_value = (code, short_name, now_date, '急跌: 5日', now_close, highest, lowest, now_range_5)
            print(slump_value)
            slump_list.append(slump_value)

    insert_sql = "insert into strategy(code, company_name, date, type , price, highest, lowest, `range`)" \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, slump_list)


# 感兴趣的组合
def interest_stock(date, strategy_type):
    query_sql = "select s.* from strategy s left join stock_price price on s.code = price.code and s.date = price.date " \
                "where s.date = %s and price.money > 100000000 and s.type = %s"
    data = my.select_all(query_sql, (date, strategy_type))

    frame = pd.DataFrame(data)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_rows', None)
    pd.set_option('max_colwidth', 200)
    print(frame)


def _get_range(now_price, highest, lowest):
    difference = highest - lowest
    if difference == 0:
        return 100
    else:
        return 100 - round((now_price - lowest) / difference, 4) * 100
