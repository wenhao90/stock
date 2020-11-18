from data import dbutils as db
import pymysql


def create_conn():
    conn = db.POOL.connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    return conn, cursor


def close_conn(conn, cursor):
    conn.close()
    cursor.close()


def select_one(sql, args):
    conn, cur = create_conn()
    cur.execute(sql, args)
    result = cur.fetchone()
    close_conn(conn, cur)
    return result


def select_all(sql, args):
    conn, cur = create_conn()
    cur.execute(sql, args)
    result = cur.fetchall()
    close_conn(conn, cur)
    return result


def insert_one(sql, args):
    conn, cur = create_conn()
    result = cur.execute(sql, args)
    conn.commit()
    close_conn(conn, cur)
    print('insert %s success: %s', sql, result)
    return result


def insert_many(sql, args):
    conn, cur = create_conn()
    result = cur.executemany(sql, args)
    conn.commit()
    close_conn(conn, cur)
    print('insert %s success: %s', sql, result)
    return result


def delete_one(sql, args):
    conn, cur = create_conn()
    result = cur.execute(sql, args)
    conn.commit()
    close_conn(conn, cur)
    return result


def delete_many(sql, args):
    conn, cur = create_conn()
    result = cur.executemany(sql, args)
    conn.commit()
    close_conn(conn, cur)
    print('delete %s success: %s', sql, result)
    return result


def update_one(sql, args):
    conn, cur = create_conn()
    result = cur.execute(sql, args)
    conn.commit()
    close_conn(conn, cur)
    return result


def update_many(sql, args):
    conn, cur = create_conn()
    result = cur.executemany(sql, args)
    conn.commit()
    close_conn(conn, cur)
    print('update %s success: %s', sql, result)
    return result

# sql = "insert into stu(id,name,age) VALUE (%s,%s,%s)"   #增加
# res = insert_one(sql, [2,"飞机", 28])
# print(res)

# sql = "delete from stu where name = %s"   #删除
# res = delete_one(sql, "哪吒")
# print(res)

# sql = "select * from stu"   #查询全部
# res = select_all(sql, [])
# print(res)

# sql = "select * from stu where name=%s"   #查询一条
# res = select_one(sql, "赵振伟")
# print(res)
