# 股票相关的操作
import jqdatasdk as sdk
from app import join_quant as jq

# JQData证券代码标准格式
# 上海证券交易所   .XSHG
# 深圳证券交易所   .XSHE

jq.login()

# 获取所有标的信息
# types
# stock(股票)
# index(指数)
# etf(ETF基金)
# data = sdk.get_all_securities(types=['stock'], date='2020-10-30')

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
# data = sdk.finance.run_query(sdk.query(sdk.finance.SW1_DAILY_PRICE).filter(sdk.finance.SW1_DAILY_PRICE.code == '801010').limit(1))

# 获取行情数据
# data = sdk.get_price('600519.XSHG', start_date='2020-10-30', end_date='2020-10-30', frequency='daily', fq='pre')

# 获取融资融券信息
# data = sdk.get_mtss('000001.XSHE', '2020-10-30', '2020-10-30')

# 获取龙虎榜数据获取龙虎榜数据
# data = sdk.get_billboard_list(stock_list=None, end_date='2020-10-30', count=1)

# 沪深市场每日成交概况
# 322002	上海A股
# 322005	深市主板
data = sdk.finance.run_query(sdk.query(sdk.finance.STK_EXCHANGE_TRADE_INFO).filter(
    sdk.finance.STK_EXCHANGE_TRADE_INFO.exchange_code == '322002').limit(1))

# 获取融资融券汇总数据
data = sdk.finance.run_query(sdk.query(sdk.finance.STK_MT_TOTAL).filter(sdk.finance.STK_MT_TOTAL.date=='2020-10-30').limit(2))


print(data)
