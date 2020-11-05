import pandas as pd
import numpy as np
import re
from datetime import datetime, date, timedelta
from dateutil.parser import parse as strptime
from lib import codelist
from lib import logger
from api import sinaApi
from db import profitLib 
import time

TODAY = (date.today()).strftime("%Y%m%d")

def prev_weekday():
    adate = datetime.today()
    adate -= timedelta(days=7)
    while adate.weekday() > 4:  # Mon-Fri are 0-4
        adate -= timedelta(days=1)
    return adate.strftime("%Y%m%d")


import os, sys, json
import code
from db import priceDb, metaDb, keyvDb, profitDb
from db.conn import pro

import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-code", default="", help="stock code")
parser.add_argument("-n", default=0, help="stock数量")
parser.add_argument("-cmd", default="")
parser.add_argument("-opt", default="")
parser.add_argument("-raw", action="store_true")
parser.add_argument("-i", "--interact", action="store_true")
parser.add_argument("-d", "--debug", action="store_true")
parser.add_argument("-nonet", "--nonetwork", action="store_true")
Args = parser.parse_args()


def interact(local):
    # interactDebug(locals())
    if Args.interact:
        import code
        import readline
        import rlcompleter

        readline.parse_and_bind("tab: complete")
        code.interact(local=local)


from functools import wraps




if Args.raw:
    pd.set_option("display.max_columns", None)  # or 1000
    pd.set_option("display.max_rows", None)  # or 1000
    pd.set_option("display.max_colwidth", -1)  # or 199
    pd.options.display.width = None

# if Args.interact: interact(local=locals())


def debug(v):
    print(v)


def genlist():
    debug(["genlist"])
    data = pro.query(
        "stock_basic", exchange="", list_status="L", fields="ts_code,name,industry"
    )
    data = data[data.apply(lambda row: "ST" not in row["name"], axis=1)]
    print(data)
    data = data.rename(index=str, columns={"ts_code": "code"}).fillna("")
    debug(data)
    metaDb.addMetaBatch(data)


def getMeanLine(vlist, nday=60):
    n = len(vlist)
    meanList = [0] * n
    s = sum(vlist[-(nday - 1) :])
    for i in range(n - nday, -1, -1):
        s += vlist[i]
        meanList[i] = s / nday
        s -= vlist[i + nday - 1]
    return meanList

next_end_date = "20190930"

from db import preprofitDb


@keyvDb.withCache(pre="disclosure", expire=86400)
def listen_disclosure(code):
    df = pro.disclosure_date(ts_code=code, end_date=TODAY)
    if not df.empty:
        row = df.iloc[0, ["ts_code", "end_date", "pre_date"]].rename(
            index={"ts_code": "code", "pre_date": "ann_date"}
        )
        if row.end_date >= next_end_date:
            row = row.to_dict()
            preprofitDb.add(row)


def pullProfit():
    for code in metaDb.getAllCode():
        print(code[:-2])
        profitLib.pullProfitCode(code)
        # metaDb.getMetaByCode(code, updateLevel=True)

def pullProfitGood():
    from lib.codelist import stockListMap
    for code in stockListMap:
        print(code)
        profitLib.pullProfitCode(code)
        # metaDb.getMetaByCode(code, updateLevel=True)


def syncProfit():
    # Sync 财报
    for row in preprofitDb.getSyncProfitList():
        code = row["code"]
        if profitDb.hasTradeProfit(row["code"], row["end_date"]):
            profitLib.pullProfitCode(row["code"])
            preprofitDb.add({"code": code, "ann_done": 1})


def showCode():
    codes = codelist.parseCodes(Args.code)
    for ts_code in codes:
        if not Args.nonetwork:
            profitLib.pullProfitCode(ts_code)
            metaDb.getMetaByCode(ts_code, updateLevel=True)
        profitDb.showCode(ts_code)
        # df = pro.express(ts_code=ts_code, start_date='20180101', end_date='20191201', fields='ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets')
        # print('6000',df)

def getGood(codes=[]):
    from db.conn import cursor
    #from lib.codelist import keji_codes
    # profitDb.updateBuy()
    # metaDb.updateLevel()

    where_codes = ""
    if len(codes):
        where_codes = " and code in (%s) " % ",".join(
            ["'" + code + "'" for code in codes]
        )

    LATEST_END_DATE = (date.today() - timedelta(days=120)).strftime("%Y%m%d")
    sql = f"select p.*,metas.name from (select distinct on (code) code,end_date,pe,peg,ny,dny,q_dtprofit_yoy,dtprofit_yoy,buy from profits where (netprofit_yoy>26 and q_dtprofit_yoy>-10 and end_date>='{LATEST_END_DATE}' and peg>1.30 and buy>=0  {where_codes}) order by code,end_date desc) p join metas on metas.code=p.code  order by p.peg desc"
    print(sql)
    cursor.execute(sql)
    rows1 = [dict(row) for row in cursor]
    rows = []
    for idx, row in enumerate(rows1):
        code = row["code"]
        # disclosure
        # listen_disclosure(code)

        # update meta
        row.update(metaDb.getMetaByCode(code, updateLevel=True))
        if row['level']<98:
            continue


        # mean 60
        # row["mean"] = priceDb.getPriceMean(row["code"], TODAY)
        rows.append(row)

    # update price
    #if 'price' in Args.opt:
    #print([row['code'] for row in rows])
    codePriceMap = sinaApi.getPriceInfoByCodes([row['code'] for row in rows])
    #priceDb.pullPrice(code)
    for row in rows:
        row["price"] = codePriceMap[row['code']]['price']
        row["change"] = 100*float(row['level_price'])/float(row["price"])-100

    cols = ['end_date','name', 'code','industry','level','price','level_price','change','dny','dtprofit_yoy','q_dtprofit_yoy','ny','pe','peg']
    #df = pd.DataFrame(rows)[cols].sort_values(by=['industry', 'level'], ascending=False)
    df = pd.DataFrame(rows)[cols].sort_values(by=['industry', 'change'], ascending=True)
    df = df.groupby('industry').head(10)
    #df = pd.DataFrame(rows)[cols].sort_values(by=['industry', 'peg'], ascending=False)
    print("goodp\n", df)


if __name__ == "__main__":
    print(Args)
    # if Args.code: single()
    if Args.cmd == "genlist":
        # exec(Args.cmd)
        # globals()[Args.cmd]()
        genlist()
    elif Args.cmd == "pullProfit":
        pullProfit()
    elif Args.cmd == "syncProfit":
        syncProfit()
    elif Args.cmd == "pullProfitGood":
        pullProfitGood()
    elif Args.cmd == "updateBuy":
        profitDb.updateBuy()
    # elif Args.cmd == "updateLevel":
    #     metaDb.updateLevel()
    elif Args.cmd == "clearKv":
        keyvDb.clearDb()
    elif Args.cmd == "getName":
        codes = codelist.parseCodes(Args.code)
        print(metaDb.getNameByCodes(codes))
    elif Args.cmd == "getGood":
        codes = codelist.parseCodes(Args.code)
        getGood(codes)
    elif Args.cmd == "getBank":
        metaDb.getBank()
    elif Args.cmd == "pullPrice":
        print("pull Price...")
        code = Args.code or "601318.SH"
        priceDb.pullPrice(metaDb.getCode(code))
    elif Args.cmd == 'show':
        Args.raw = True
        showCode()
    else:
        quit('error cmd:'+Args.cmd)
    logger.lg("done")
    quit()

