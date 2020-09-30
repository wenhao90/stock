# 股票相关的操作
import jqdatasdk as sdk
from app import join_quant as jq
from jqlib.technical_analysis import *


# 根据指数获取基本股票
def get_stock_by_code(code_str):
    jq.login()
    stock = sdk.get_security_info(code_str)
    return stock


jq.login()
# print (sdk.get_security_info('000001.XSHE'))
#行业信息
# data = sdk.get_industry("000001.XSHE")
# 行业列表
# data  = sdk.get_industries(name='jq_l1', date=None)
# 股票行业信息
# data  = sdk.get_industry_stocks('HY003')
# 融资融券余额
# data = sdk.finance.run_query(sdk.query(sdk.finance.STK_MT_TOTAL).filter(sdk.finance.STK_MT_TOTAL.date=='2020-09-29').limit(1))
# ah对比
# data = sdk.finance.run_query(sdk.query(sdk.finance.STK_AH_PRICE_COMP).filter(sdk.finance.STK_AH_PRICE_COMP.a_code=='000002.XSHE').order_by(sdk.finance.STK_AH_PRICE_COMP.day).limit(1))
# 按周期获取行情
# data = sdk.get_bars('600519.XSHG', 10, unit='10m',fields=['date','open','high','low','close','volume','money'],include_now=False,end_dt='2020-09-30 14:00:00')
# data = sdk.get_price('600519.XSHG', start_date=None, end_date="2020-09-30 14:00:00", frequency='minute', fields=None, skip_paused=False, fq='pre', count=10, panel=True, fill_paused=True)
#股票信息
# data = dict(sdk.finance.run_query(sdk.query(sdk.finance.STK_COMPANY_INFO).filter(sdk.finance.STK_COMPANY_INFO.code=='600519.XSHG').limit(1)))
data = sdk.EMA(['600519.XSHG'], check_date='2020-09-29', timeperiod=30)
print(data)

