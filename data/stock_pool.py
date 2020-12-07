# 股票池

from data import mysql_util as my


def stock_add(code):
    insert_sock = "insert into pool(code, company_name,highest,lowest,level) select code, short_name,highest,lowest,1 from security where code = %s"
    my.insert_one(insert_sock, code)

    now_price_sql = "select close from stock_price where code = %s order by date desc limit 1"
    now_price = my.select_one(now_price_sql, code)
    price = now_price['close']


    update_price_sql = "update pool set add_price = %s, now_price = %s, ratio = %s, `range` = (100 - round((now_price - lowest) / (highest - lowest), 4) * 100) where code = %s"
    my.update_one(update_price_sql, (price, price, 100, code))

    update_strategy = "update pool set strategy = (select group_concat(type) from strategy where code = %s and date = (select date FROM stock_price where code = %s order by date desc limit 1)) where code = %s"
    my.update_one(update_strategy, (code, code, code))


def stock_update():
    pool_sql = "select code from pool where level > 0"
    pool_data = my.select_all(pool_sql, ())

    for index_pool in pool_data:
        code = index_pool['code']

        update_price = "update pool set now_price = (select close from stock_price where code = %s order by date desc limit 1)  where code = %s"
        my.update_one(update_price, (code, code))

        update_ratio_sql = "update pool set ratio = round(now_price / add_price, 4) * 100, `range` = (100 - round((now_price - lowest) / (highest - lowest), 4) * 100) where code = %s"
        my.update_one(update_ratio_sql, code)

        update_strategy = "update pool set strategy = (select group_concat(type) from strategy where code = %s and date = (select date FROM stock_price where code = %s order by date desc limit 1))  where code = %s"
        my.update_one(update_strategy, (code, code, code))
