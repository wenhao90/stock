# 根据数据处理一些策略

from data import mysql_util as my


# 市场宽度(统计每个行业中，股票收盘价大于20日移动平均线的比值)
def strategy_market_width(industry_type='sw_l1', start_date=None):
    width_sql = "select industry.industry_name, industry.industry_code, price.date, count(1) count, " \
                "count(if(price.close > price.sma_20, 1, null)) success " \
                "from stock_price price left join stock_industry industry on price.code = industry.code " \
                "where industry.type = %s and price.date >= %s " \
                "group by industry.industry_name, industry.industry_code, price.date " \
                "order by price.date desc"
    width_data = my.select_all(width_sql, (industry_type, start_date))

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
def strategy_low_value(date):
    price_sql = "select stock.code, stock.short_name,price.date,price.close,stock.highest,stock.lowest " \
                "from stock_price price left join security stock on price.code = stock.code " \
                "where price.date = %s"
    price_data = my.select_all(price_sql, date)

    low_value_list = []
    for index_price in price_data:
        close = float(index_price['close'])
        highest = float(index_price['highest'])
        lowest = float(index_price['lowest'])
        print("close: %s, highest: %s, lowest: %s", close, highest, lowest)

        difference = highest - lowest
        low_range = 100
        if difference != 0:
            low_range = 100 - round((close - lowest) / difference, 4) * 100

        if low_range > 70:
            code = index_price['code']
            short_name = index_price['short_name']
            date = index_price['date']

            low_value = (code, short_name, date, '低值', close, highest, lowest, low_range)
            low_value_list.append(low_value)

    print(low_value_list)
    insert_sql = "insert into strategy(code, company_name, date, type , price, highest, lowest, `range`)" \
                 " values (%s, %s, %s, %s, %s, %s, %s, %s)"
    my.insert_many(insert_sql, low_value_list)


# 每天3点，更新完股票价格之后
# strategy_market_width(start_date='2020-09-01')


# 每天3点，更新完股票价格之后
# strategy_low_value('2020-11-11')

# 低值查询
# "select s.* from strategy s left join stock_price price on s.code = price.code and s.date = price.date where `range` > 90 and price.money > 100000000"
