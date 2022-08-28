import pandas as pd
import time,sys
sys.path.append('.')
from datetime import datetime, date, timedelta
from conf import conf
from lib import codelist
from lib import logger
from api import sinaApi,goodLevelApi
from lib import profitLib 

TODAY = (date.today()).strftime("%Y%m%d")

def prev_weekday():
    adate = datetime.today()
    adate -= timedelta(days=7)
    while adate.weekday() > 4:  # Mon-Fri are 0-4
        adate -= timedelta(days=1)
    return adate.strftime("%Y%m%d")


import code
from db import priceDb, metaDb, keyvDb, profitDb
from db.conn import getProApi

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
    pd.set_option("display.max_colwidth", None)  # or 199
    pd.options.display.width = None

# if Args.interact: interact(local=locals())


def debug(v):
    print(v)


def genlist():
    debug(["genlist"])
    data = getProApi().query(
        "stock_basic", exchange="", list_status="L", fields="ts_code,name,industry"
    )
    #data = data[data.apply(lambda row: "ST" not in row["name"], axis=1)]
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
    logger.log('showCodes:',codes)
    for ts_code in codes:
        if not Args.nonetwork:
            mainRow = goodLevelApi.getIndicatorByCode(ts_code)
            print("profile:\n",mainRow)
            # metaDb.getMetaByCode(ts_code, updateLevel=False)
        profitDb.showCode(ts_code)
        # print('6000',df)


def getGood(codes=[]):
    MIN_LEVEL = 20
    MIN_DNY= 1.055 # 最近一年的业绩
    print("dny>",MIN_DNY)
    print("levle>",MIN_LEVEL)
    from db.conn import cursor
    # LATEST_END_DATE = (date.today() - timedelta(days=160)).strftime("%Y%m%d")
    # sql = f"select p.*,metas.name from (select distinct on (code) code,end_date,pe,peg,dny,tr,try,buy from profits where (dny>1.20 and try>1.20 and end_date>='{LATEST_END_DATE}' {where_codes}) order by code,end_date desc) p join metas on metas.code=p.code  order by p.peg desc"
    # cursor.execute(sql)
    # rows1 = [dict(row) for row in cursor]
    # 预期good
    rows  = []
    highLevelStocks = goodLevelApi.getGoodLevelStocks(0.20, Args.code)
    cols = ['code','name','rateEps','rateEps4','EPS1', 'EPS2','level']
    if len(highLevelStocks)==0:
        quit('no good stocks 1')
    highLevelStockDf = pd.DataFrame(highLevelStocks)
    highLevelStockDf = highLevelStockDf.rename(columns={ 
        "stockCode": "code",
        "stockName": 'name',
        "level":"level",
    })
    # print(highLevelStockDf.columns)
    highLevelStockDf = highLevelStockDf[cols]
    highLevelStockDf.index = highLevelStockDf['code']
    
    for _, row in highLevelStockDf.iterrows():
        # row = row.loc[['code,name,level'.split(',')]]
        code = row["code"]
        # 0. filter code
        if code == '000043':
            continue
        code = codelist.parseCodes(code)[0]
        # meta+profit
        mainRow = goodLevelApi.getIndicatorByCode(code)
        row = {**mainRow, **row}
        level = row['level']
        # 1. level
        if level<MIN_LEVEL:
            continue

        # 2. dny
        # if 'dny' not in row: continue
        if row['dny']<MIN_DNY:
            print("skip dny:%s name:%s %s" % (row['dny'], code,row['name']))
            continue

        # 3. profit
        if row['buy']==0:
            print("skip buy name:%s %s" % (code, row['name']))
            continue
        # 3. profit
        if row['rateEps4']<0.85:
            print("skip rateEps4:%f name:%s %s" % (row['rateEps4'],code, row['name']))
            continue
        # 3. try 营业收入增长TTM
        # if row['try']<1.20: continue
        # code,end_date,dtprofit,q_dtprofit,dny,tr,try
        # print(row)
        rows.append(row)

    if len(rows) == 0:
        quit('No good stocks 2')

    # update price
    codePriceMap = sinaApi.getPriceInfoByCodes([row['code'] for row in rows])
    for row in rows:
        row["price"] = codePriceMap[row['code']]['price']
        row['end_date']=datetime.fromtimestamp(row['end_date']/1000)
        # row["change"] = 100*float(row['level_price'])/float(row["price"])-100

    cols = ['end_date','name', 'code','industry','rateEps','level','price','dny','dtprofit_yoy','q_dtprofit_yoy','peg']
    # 3. try 营业收入增长
    cols = ['end_date','name', 'code','industry','rateEps','rateEps4','price','dny','peg','try']
    #df = pd.DataFrame(rows)[cols].sort_values(by=['industry', 'level'], ascending=False)
    # df = pd.DataFrame(rows)[cols].sort_values(by=['rateEps'], ascending=False)
    df = pd.DataFrame(rows)[cols].sort_values(by=['industry', 'rateEps'], ascending=False)
    df = df.groupby('industry').head(2)
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
    logger.log("done")
    quit()

