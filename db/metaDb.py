from db.conn import cursor
from datetime import datetime, date, timedelta
from api import sinaApi
from db import keyvDb
from lib import logger


def getCode(ts_code):
    if "." not in ts_code:
        ts_code += ".SH" if ts_code[0] == "6" else ".SZ"
    return ts_code


def addMeta(data):
    cursor.insertUpdate("metas", data, "code,trade_date")

def addMetaBatch(meta_list):
    # if str(type(meta_list)) == "<class 'pandas.core.frame.DataFrame'>":
    #     meta_list["update_time"] = datetime.now()
    cursor.insertBatch("metas", meta_list, "code")


def getAllCode():
    cursor.execute("select code from metas ")
    return [row["code"] for row in cursor]


def getCodesByNames(names):
    cursor.execute("select code,name from metas where name=ANY(%s)", [names])
    res = {}
    for row in cursor:
        res[row["name"]] = row["code"]
    return res


def getCodeByName(name):
    cursor.execute("select code from metas where name=%s limit 1", [name])
    res = cursor.fetchone()
    if res:
        return res["code"]


def getNameByCodes(codes):
    cursor.execute("select code,name from metas where code=ANY(%s)", [codes])
    res = {}
    for row in cursor:
        res[row["code"]] = row["name"]
    return res
def patchMetaInfo(stockList):
    codes = [r['code'] for r in stockList ]
    cursor.execute("select code,name,industry from metas where code=ANY(%s)", [codes])
    res = {}
    for row in cursor:
        res[row["code"]] = row
    for item in stockList:
        item.update(res[item['code']])


def getNameByCode(code):
    cursor.execute("select name from metas where code=%s limit 1", [code])
    res = cursor.fetchone()
    if res:
        return res["name"]


def getGoodCode():
    cursor.execute("select code from metas where peg<0.5")
    return [row["code"] for row in cursor]


def getMetaList(page=1, size=20):
    offset = (page - 1) * size
    cursor.execute("select*from metas where limit %s offset %s", [size, offset])
    return cursor.fetchall()



# @private
# @keyvDb.withCache("syncMetaLevel", 86400 * 1)
def syncMetaLevel(code):
    print('sync meta:'+code)
    # p_change/level/level_price/90mean
    row = {"code": code}
    level = sinaApi.getLevel(code)
    print(level)
    row = {**row, **level}
    row["update_time"] = datetime.now()
    cursor.insertUpdate("metas", row, "code")


def getMetaByCode(code, updateLevel=False,expire=86400*30):
    cursor.execute("select*from metas where code=%s ", [code])
    row = cursor.fetchone()
    if updateLevel and ((datetime.now() - row["update_time"]).total_seconds() > expire or row['level_price']==0):
        syncMetaLevel(code)
        cursor.execute("select*from metas where code=%s ", [code])
        row = cursor.fetchone()
    return row

def getBank():
    from db.conn import cursor
    import pandas as pd
    # profitDb.updateBuy()
    # metaDb.updateLevel()

    LATEST_END_DATE = (date.today() - timedelta(days=150)).strftime("%Y%m%d")
    sql = f"select p.*,metas.name from (select distinct on (code) code,end_date,pe,peg from profits where pe<13 order by code,end_date desc) p join metas on metas.code=p.code where metas.name like '%银行' order by p.pe desc"
    print(sql)
    cursor.execute(sql)
    rows = []
    for row in cursor:
        rows.append(dict(row))
    df = pd.DataFrame(rows)
    print(df)

if __name__ == "__main__":
    syncMetaLevel("601318.SH") # __main__
