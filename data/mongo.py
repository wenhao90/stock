# mongo 相关的操作
import pymongo


# 获取集合
def get_col(col):
    client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = client["stock"]

    col = db[col]
    return col


# 获取总数
def get_count(col):
    return get_col(col).find().count()


# 插入一条数据(需要将数据转化为 json)
def insert_one(col, data):
    return get_col(col).insert_one(data)


# 插入多条数据(需要将数据转化为 json)
def insert_many(col, data):
    return get_col(col).insert_many(data)


# 查询一条数据
def find_one(col, query={}):
    return get_col(col).find_one(query)


# 查询一条数据
def find_many(col, query={}):
    return list(get_col(col).find(query))
