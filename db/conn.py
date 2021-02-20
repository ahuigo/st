import psycopg2
import psycopg2.extras
from conf.conf import dbconf
from datetime import datetime
import tushare as ts
#import akshare as ak
conn = None

"""
start api
"""

pro = None
def getProApi():
    global pro
    if pro is None:
        key = open("conf/ts.key").read().strip()
        pro = ts.pro_api(key)
        pro.pro_bar = ts.pro_bar
    return pro

"""
Db
"""


# cursor.insertBatch('prices', rows, 'time,code')
def insertBatch(cursor, table, rows, onConflictKeys=None):
    if len(rows) <= 0:
        return
    if str(type(rows)) == "<class 'pandas.core.frame.DataFrame'>":
        keys = list(rows.keys())
        values = rows.values
    else:
        keys = list(rows[0].keys())
        values = [list(row.values()) for row in rows]

    key_fields = ",".join(keys)
    value_format = ",".join(["%s"] * len(keys))

    sql = f"insert into {table}({key_fields}) values({value_format})"
    if onConflictKeys:
        update_keys = set(keys) - set(onConflictKeys.split(","))
        if len(update_keys) > 0:
            sql += f" ON CONFLICT({onConflictKeys})"
            sql += f" DO UPDATE set " + ",".join(
                [key + "=EXCLUDED." + key for key in keys]
            )
        else:
            sql += f" ON CONFLICT DO NOTHING"
    # print(sql, values)
    try:
        cursor.executemany(sql, values)
    except Exception as e:
        print(cursor.query)
        raise e




# onConflictKeys="key1,key2"
def insertUpdate(cursor, table, row, onConflictKeys=''):
    keys = tuple(row.keys())
    values = tuple(row.values())

    key_fields = ",".join(keys)
    value_format = ",".join(["%s"] * len(keys))
    conflictKeys= onConflictKeys.split(",")

    sql = f"insert into {table}({key_fields}) values({value_format})"
    if onConflictKeys:
        update_keys = set(keys) - set(onConflictKeys.split(","))
        if len(update_keys) > 0:
            sql += f" ON CONFLICT({onConflictKeys})"
            sql += f" DO UPDATE set " + ",".join(
                [key + "=EXCLUDED." + key for key in keys]
            )
        else:
            sql += f" ON CONFLICT DO NOTHING"
    # print(sql, values)
    try:
        cursor.execute(sql, values)
    except psycopg2.errors.UniqueViolation as e:
        if not onConflictKeys:
            raise e
        print([cursor.query])
        conflictKeys += uk

        sql = f'update {table} set'
        set_keys = ','.join([f'"{k}"=%s' for k in keys])
        where_keys = ' and '.join([f'"{k}"=%s' for k in conflictKeys])
        values += [row[k] for k in conflictKeys]
        sql = f'{sql} {set_keys} where {where_keys}'
        cursor.execute(sql, values)

    except Exception as e:
        print([cursor.query])
        raise e

# {database, user,  password, host, port}
def getDbCursor(dbconf):
    global conn
    conn = psycopg2.connect(**dbconf)
    conn.set_session(readonly=False, autocommit=True)
    # cursor = conn.cursor()
    cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cursor.insertBatch = insertBatch.__get__(cursor)
    cursor.insertUpdate = insertUpdate.__get__(cursor)
    return cursor

cursor = getDbCursor(dbconf)

if __name__ == "__main__":
    rows = [
        {"time": "20190901", "code": "sz0001", "price": 2},
        {"time": "20190902", "code": "sz002", "price": 3},
    ]
    # cursor.insertBatch('prices', rows, 'time,code')
    import pandas as pd

    # rows = pd.DataFrame([{'code':'00000.XX','name':'股票','industry':'科技'}])
    row = {"op": "0001", "action": "profit", "time": datetime.today()}
    cursor.insertUpdate("oplog", row)

