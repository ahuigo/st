from typing import List
from db.conn import cursor
from datetime import datetime, date, timedelta
import pandas as pd
from . import exchangeDb
from api import sinaApi
from db import keyvDb
from db.conn import getProApi
from conf.conf import DEBUG


def add(trade_date, code, price):
    err = cursor.execute(
        "insert into  prices values(%s,%s,%s)", [trade_date, code, price]
    )
    return err


def addBatch(price_list):
    cursor.insertBatch("prices", price_list, "code,trade_date")


"""
def getPriceMean(code, trade_date, nday=60):
    cursor.execute(f"select close from prices where code=%s and trade_date<=%s limit {nday}", [code, trade_date, ])
    rows = [dict(row) for row in cursor.fetchall()]
    if rows:
        df = pd.DataFrame(rows)
        return df['close'].sum()/nday
"""


def getLatestPriceRow(code):
    cursor.execute(
        "select * from prices where code=%s order by trade_date desc limit 1", [code]
    )
    return cursor.fetchone()


def getLatestPrice(code):
    return getLatestPriceRow(code)[0]


def getPricesByCode(code, page=1, size=1000):
    offset = (page - 1) * size
    cursor.execute(
        "select code,trade_date,high,low,close from prices where code=%s order by trade_date desc limit %s offset %s",
        [code, size, offset],
    )
    rows = cursor.fetchall()
    for i, row in enumerate(rows):
        rows[i] = {**row, "close": float(row["close"])}
    rows.reverse()
    return rows


def getPricesByDate(trade_date, codes):
    stockList = []
    cursor.execute(
        "select distinct on(code) code,close as close from prices where trade_date=%s and code = ANY (%s) ",
        [trade_date, codes],
    )
    # rows = cursor.fetchall()
    for row in cursor:
        row = dict(row)
        row.update(
            {"close": float(row["close"]), "num": 0,}
        )
        stockList.append((row))
    return stockList


# 历史价
@keyvDb.withCache("pullPrice", 64800)
def pullPrice(code, start_date=""):
    if not start_date:
        row = getLatestPriceRow(code)
        start_date = row["trade_date"].strftime("%Y%m%d") if row else "20190501"
    # if start_date < exchangeDb.getTradeDate():
    data = getProApi().pro_bar(api=getProApi(), ts_code=code, adj="qfq", start_date=start_date)
    # data=data[data.apply(lambda row:'ST' not in row['name'], axis=1)]
    data = (
        data[["ts_code", "trade_date", 'high','low',"close"]]
        .rename(index=str, columns={"ts_code": "code"})
        .fillna("")
    )
    # data['mean'] = getMeanLine(data['close'].to_list(), 60)
    addBatch(data)
    return True


# 最新价
def getPullPricesByCode(code, page=1, size=(180 * 5 / 7) // 1):
    if not DEBUG:
        pullPrice(code)
    prices = getPricesByCode(code, page, size)
    if prices:
        price = sinaApi.getCurPriceByCode(code)
        prices.append(
            {"code": code, "trade_date": date.today(), "close": price,}
        )

    return prices


def getPullPricesByCodeList(codes:List[str], page=1, size=(180 * 5 / 7)):
    res = {}
    for code in codes:
        res[code] = getPullPricesByCode(code, page, size)
    return res

# def getCurPriceByCode(code):
#     price = sinaApi.getCurPriceByCode(code)
#     return price
