# 业务操作
from data import stock_sync_price as price
from data import stock_strategy as strategy
from data import stock_pool as pool
from app import join_quant as jq

# 剩余条数
# jq.remainder()

######### 获取实时数据 ######################################################################
# 获取股票价格(每天3点以后)
# price.get_stock_price(start_date='2020-11-26', end_date='2020-11-26')

# 生成SMA数据(每天3点,更新价格以后)
# price.get_sma_20(1)


########## 策略更新(跟新万股票信息之后) #####################################################################
# 更新市场宽度
# strategy.strategy_market_width()

# 低值: 每天3点，更新完股票价格之后
# strategy.strategy_low_value()

# 大量: 每天3点，更新完股票价格之后
# strategy.strategy_volume_large()

# 破线、拐头: 每天3点，更新完股票价格之后
# strategy.strategy_ma()

# 急跌: 每天3点，更新完股票价格之后
# strategy.strategy_slump()

# 缺口: 每天3点，更新完股票价格之后
# strategy.strategy_gap()

# 组合查询
# strategy.interest_stock('2020-11-16', '破线拐头')

########## 获取不实时信息 #####################################################################
# 获取龙虎榜(早上获取前一天数据)
# price.get_billboard(start_date='2020-11-24', end_date='2020-11-24')

# 获取行业数据(早上获取前一天数据)
# price.get_index_price(1)

# 获取市场融资融券情况(早上获取前一天数据)
# price.get_market_total(1)


########## 股票池更新 #####################################################################
# pool.stock_add('601989.XSHG')
# pool.stock_update()


########## 获取财务数据(一季度) #####################################################################
# get_finance_data('2020-10-30')
