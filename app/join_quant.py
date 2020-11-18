# 聚宽自身操作
import jqdatasdk as jq


# 登录
# 13547972590
# 15680771810
def login(name="15680771810", password="aa645549788WH"):
    jq.auth(name, password)


def remainder():
    login()

    count = jq.get_query_count()
    print(count)
