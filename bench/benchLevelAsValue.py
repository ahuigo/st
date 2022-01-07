from db.conn import cursor
from db import priceDb
from datetime import datetime, date, timedelta
import math
from copy import deepcopy
from lib import MeanLine
from lib import logger
import sys, os
from lib.strategyBase import (
    createStrategy,
    execStrategy,
    calc_sell,
    calc_buy,
    calc_total,
    debug_stockList,
)
from conf.conf import DEBUG
from conf import conf


BALANCE = 60 * 1e4
max_hold_n = 10
BUY_RATE = 1.001
SELL_RATE = 0.998

# 交易开始时间
start_trade_date = date(2020, 3, 11)
#start_trade_date = date(2020, 2, 21)
start_trade_date = date(2020, 2, 14)
end_trade_date = date(2020, 7, 4)
# 交易天数
trade_day_num = (date.today() - start_trade_date).days
# 开始前参考时间
sys.stderr.write(f"start_trade_date:{start_trade_date}\n")


"""
先取全部的价格
"""
from collections import defaultdict

all_prices = defaultdict(dict)
all_stocks = defaultdict(list)


def gen_all_prices(codes, start_trade_date):
    cursor.execute(
        "select distinct on(code) code from prices where code=any(%s) and trade_date>=%s ",
        [codes,end_trade_date],
    )
    print(cursor.query)
    codes1 = [row['code'] for row in cursor.fetchall()]
    for code in (set(codes) - set(codes1)):
        print('pull price code', code)
        priceDb.pullPrice(code)

    cursor.execute(
        "select code,trade_date,close,high,low from prices where code=any(%s) and trade_date>=%s order by code,trade_date",
        [codes,start_trade_date],
    )
    for row in cursor:
        trade_date = row["trade_date"]
        code = row["code"]
        priceInfo = {**row, "close": float(row["close"])}
        all_prices[trade_date][row["code"]] = priceInfo
        all_stocks[code].append(priceInfo)
    # gen_all_prices_mean(all_prices, mean_period)


def gen_yestclose(all_stocks):
    for code, prices in all_stocks.items():
        # MeanLine.setEvalueLine(prices)
        mean = prices[0]['close']
        for priceInfo in (prices):
            priceInfo['mean'] = mean;
            mean = priceInfo['close']

def setLevelPrice(stockList, price_key='price'):
    from api import sinaApi
    from db import metaDb
    # codePriceMap = sinaApi.getPriceInfoByCodes(stockListMap.keys())
    for stockInfo in stockList:
        code = stockInfo['code']
        stockInfo['level_price'] = stockInfo['close']
        continue
        print(stockInfo)
        quit()
        # price = codePriceMap[code][price_key]
        metaInfo = metaDb.getMetaByCode(code, updateLevel=False)
        # metaInfo = sinaApi.getLevel(code)
        level_price = float(metaInfo['level_price'])
        if not level_price:
            logger.log('No level_price',metaInfo,stockInfo)
            quit('Error here')
            # level_price = 0.9
        stockInfo['level_price'] = level_price
        stockInfo['level'] = metaInfo['level']
    return



def getMeanPriceByCode(code, cur_date):
    prices = all_stocks[code]
    i = 0
    j = len(prices) - 1
    while i < j:
        m = (i + j) // 2
        # print(prices[m])
        if prices[m].get("trade_date") > cur_date:
            j = m
        elif prices[m]["trade_date"] == cur_date:
            return prices[m]["mean"]
        else:
            if i == m:
                break
            i = m
    if prices[j]["trade_date"] == cur_date:
        return prices[j]["mean"]
    logger.log({"code": code, "date": cur_date})
    quit("hahah")


def exec_trade(trade_date, stockList, balance, min_change=1, step=0, max_hold_n=10):
    # 1. get state: stocklist, close
    # code:, close:,change:, state: null/hold/lock, strategy:buy/sell/null, num:23
    prices = all_prices.get(trade_date, {})
    if len(prices) == 0:
        return stockList, balance

    for row in stockList:
        code = row["code"]
        row["strategy"] = ""

        if code in prices:
            priceInfo = prices[code]
            # oprice = getMeanPriceByCode(code, trade_date)
            row["close"] = priceInfo['close']
            close = row["close"] 
            row["high"] = priceInfo['high']
            row["low"] = priceInfo['low']
            row["mean"] = row["level_price"] #priceInfo['mean']
            row["lock"] = 0
            row["change"] = 100 * close / row["mean"] - 100
        else:
            row["lock"] = 1

    # logger.log('print',stockList)
    # quit()

    createStrategy(stockList, balance, max_hold_n, min_change, step=step)
    balance = execStrategy(stockList, balance, step=step)

    if conf.min_step <= step <= conf.max_step or conf.big:
        print("step:", step)
        debug_stockList(stockList)
        print("New end balance", balance, "\n")
    return stockList, balance

def createStrategy1(
    stockList, balance, max_hold_n=2, min_change=1, debug=False, step=0,
):
    BUY_RATE = 1.001
    SELL_RATE = 0.998
    total_money = (
        sum([item["num"] * item["close"] * 0.998 for item in stockList]) + balance
    )
    avg_stock_money = 0.98 * total_money / max_hold_n

    # 1.1 init + sort
    for stockInfo in stockList:
        stockInfo["strategy"] = ""

    # 买入
    for stockInfo in stockList:
        if balance > 0:
            close = stockInfo["close"]
            high = stockInfo["high"]
            low = stockInfo["low"]
            mean = stockInfo["mean"]
            if 100*(1 - low/mean)<=min_change:
                continue
            # 太贵
            n = avg_stock_money / (100 * close * BUY_RATE)
            if n < 0.7:
                continue
            # 钱不够
            buyNum = round(n) * 100
            cost = calc_buy(stockInfo["code"], buyNum, stockInfo["close"])
            if cost > balance:
                continue
            balance -= cost
            stockInfo.update(
                {"strategy": "buy", "buyNum": buyNum, "i": 0, "cost": cost,}
            )
        else:
            break
            
    # sell
    for stockInfo in stockList:
        if balance > 0:
            close = stockInfo["close"]
            high = stockInfo["high"]
            low = stockInfo["low"]
            mean = stockInfo["mean"]
            if 100*(high/mean-1)<=min_change:
                continue
            sellNum = stockInfo['num']
            if sellNum==0 or stockInfo['lock']:
                continue
            cost = calc_sell(stockInfo["code"], sellNum, stockInfo["close"])
            balance += cost
            stockInfo.update(
                {"strategy": "sell", "sellNum": sellNum, "i": 0, "cost": cost,}
            )
        else:
            break

    return stockList


import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--change", default="1:2", type=str)
parser.add_argument("--hold", default="1:2", type=str)
parser.add_argument("--dstep", default="9:8", type=str)
parser.add_argument("-d", "--debug", action="store_true")
Args = parser.parse_args()

if __name__ == "__main__":
    min_change = Args.change
    [conf.min_step, conf.max_step] = list(map(int, Args.dstep.split(":")))
    [min_hold, max_hold] = list(map(int, Args.hold.split(":")))
    [min_change, max_change] = list(map(int, Args.change.split(":")))
    change_range = range(min_change, max_change)


    # codes +
    from lib.codelist import stockListMap
    codes = list(stockListMap.keys())
    # all_prices+all_code_prices
    gen_all_prices(codes, start_trade_date)

    # oriStockList with LevelPrice
    stockList = priceDb.getPricesByDate(start_trade_date, codes)
    setLevelPrice(stockList)
    ori_stock_list = deepcopy(stockList)

    # mean
    # gen_yestclose(all_stocks)

    print('balance',BALANCE)
    mmax = 0
    mmax_info = ''
    for min_change in change_range:
        total1 = 0
        for max_hold_n in range(min_hold, max_hold, 1):
            period = max_hold_n
            balance = BALANCE
            stockList = deepcopy(ori_stock_list)
            for i in range(0, trade_day_num, 1):
                [stockList, balance] = exec_trade(
                    start_trade_date + timedelta(days=i),
                    stockList,
                    balance,
                    min_change,
                    step=i,
                    max_hold_n=max_hold_n
                )
                if conf.Dtrade:
                    total = calc_total(stockList, balance)
                    print(f"step{i}:change-period-total:", min_change, period, total)
                if i >= conf.max_step >= conf.min_step:
                    break

            total = calc_total(stockList, balance)
            total1 += total
        total1 /= max_hold - min_hold 
        #print(f"endstep({i}):change-period-total:", min_change, period, total)
        print(f"endstep({i}):change-total:", min_change, total1)
        if mmax<total1:
            mmax=total1
            mmax_info = (f"endstep({i}):mchange-total:", min_change, total1)

    print(mmax, mmax_info)

