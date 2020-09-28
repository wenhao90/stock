# 股票相关的操作
import jqdatasdk as sdk
from app import join_quant as jq


# 根据指数获取基本股票
def get_stock_by_code(code_str):
    jq.login()
    print("python3 调用 get_security_info 函数")
    stock = sdk.get_security_info(code_str)
    print("返回值: ", stock)
    return stock


get_stock_by_code("000001.XSHE")
