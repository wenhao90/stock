# 业务操作
from data import stock_sync_price as price
from data import stock_strategy as strategy

######### 获取实时数据 ######################################################################
# 获取股票价格(每天3点以后)
# price.get_stock_price(start_date='2020-11-12', end_date='2020-11-16')

# 生成SMA数据(每天3点,更新价格以后)
# price.get_sma_20(3)

# 获取行业数据(每天3点以后)
# price.get_index_price(3)

# 获取市场融资融券情况(每天3点以后)
# price.get_market_total(3)


########## 策略更新(跟新万股票信息之后) #####################################################################
# 更新市场宽度
# strategy.strategy_market_width(start_date='2020-11-12')

# 低值: 每天3点，更新完股票价格之后
# strategy.strategy_low_value('2020-11-16')

# 缺口: 每天3点，更新完股票价格之后
# strategy.strategy_gap()

# 大量: 每天3点，更新完股票价格之后
# strategy.strategy_volume_large()

# 破线、拐头: 每天3点，更新完股票价格之后
# strategy.strategy_ma()

# 急跌: 每天3点，更新完股票价格之后
# strategy.strategy_slump()

# 组合查询
strategy.interest_stock('2020-11-16', '大量')

########## 获取龙虎榜(每天,不实时) #####################################################################
# 获取龙虎榜(时间不确定，不实时)
# price.get_billboard(start_date='2020-11-12', end_date='2020-11-16')


########## 获取财务数据(一季度) #####################################################################
# get_finance_data('2020-10-30')
