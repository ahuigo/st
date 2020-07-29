from db import metaDb, priceDb
import re, os
from datetime import date
import math
from lib import logger
from lib import MeanLine
from lib import strategyBase as strategy
from lib import codelist

from lib.strategyBase import (
    calc_sell,
    calc_buy,
    calc_total,
    debug_stockList,
)

BUY_RATE = 1.001
SELL_RATE = 0.998
HIGH_RATE = 1.03
LOW_RATE = 0.97


def sellStock(prePriceInfo, priceInfo):
    balance = prePriceInfo["balance"]
    num = prePriceInfo["num"]
    code = prePriceInfo["code"]
    mean = prePriceInfo["mean"]
    highPrice = priceInfo["close"] * HIGH_RATE
    lowPrice = priceInfo["close"] * LOW_RATE
    if mean <= highPrice and num > 0:
        balance += strategy.calc_buy(code, num, mean)
        num = 0
    else:
        prePriceInfo["strategy"] = ""
    priceInfo.update({"balance": balance, "num": num})


def buyStock(prePriceInfo, priceInfo):
    balance = prePriceInfo["balance"]
    num = prePriceInfo["num"]
    code = prePriceInfo["code"]
    mean = prePriceInfo["mean"]
    highPrice = priceInfo["close"] * HIGH_RATE
    lowPrice = priceInfo["close"] * LOW_RATE
    if lowPrice < mean and num == 0:
        n = (balance / (100 * mean * BUY_RATE)) // 1
        num = n * 100
        cost = strategy.calc_buy(code, num, mean)
        balance -= cost
    else:
        prePriceInfo["strategy"] = ""
    priceInfo.update({"balance": balance, "num": num})


def runStrategy(prices, balance):
    prePriceInfo = prices[0]
    mul = balance / prePriceInfo["close"]
    prePriceInfo.update(
        {"balance": balance, "num": 0, "strategy": None,}  # hold  # next day
    )
    for priceInfo in prices:
        # 1. do buy-sell action by prePriceInfo
        if prePriceInfo["strategy"] == "sell":
            sellStock(prePriceInfo, priceInfo)
        elif prePriceInfo["strategy"] == "buy":
            buyStock(prePriceInfo, priceInfo)
        else:
            priceInfo.update(
                {"balance": prePriceInfo["balance"], "num": prePriceInfo["num"],}
            )
        priceInfo["total"] = (
            priceInfo["balance"] + priceInfo["num"] * priceInfo["close"]
        ) / mul + 0

        # 2. gen strategy today
        if priceInfo["mean"] > prePriceInfo["mean"]:
            priceInfo["strategy"] = "buy"
        elif priceInfo["mean"] < prePriceInfo["mean"]:
            priceInfo["strategy"] = "sell"
        else:
            priceInfo["strategy"] = ""

        # 3. init state
        prePriceInfo = priceInfo


def createStrategy(stockListMap, period=30, balance=10e4):
    codes = list(codelist.stockListMap.keys())
    all_prices = priceDb.getPullPricesByCodeList(codes, 1, period + 2)
    # codeNames = metaDb.getNameByCodes(codes)
    strategies = {}
    for code, prices in all_prices.items():
        MeanLine.setMeanLine(prices, period)
        pre = prices[-2]
        cur = prices[-1]
        stock = stockListMap[code]
        num = stock["num"]
        if pre["mean"] > pre["mean"] and num == 0:
            num = round(balance / cur["close"] / 100)
            strategy = "buy"
        elif pre["mean"] < pre["mean"] and num > 0:
            strategy = "sell"
        else:
            strategy = "null"

        stock.update(
            {"strategy": strategy, "close": cur["close"], "num": num,}
        )
        print(f'{code}:{num}*{cur["close"]} -> {strategy}')
    return stockListMap


def main(code='600036.SH', period=30, balance=10e4):
    import math
    # 1.get prices
    prices = priceDb.getPricesByCode(code, 1, 197)
    pre_priceInfo = None
    balance = 100*1e4
    percent = 0.75
    holdn = 0
    for priceInfo in prices:
        if pre_priceInfo is None:
            pre_priceInfo = priceInfo
            print(priceInfo)
            continue
        price = priceInfo['close']
        holdv = holdn*price
        total = balance + holdv
        holdv_should = total * percent
        msg = ''
        # 亏 - buy
        if holdv_should - holdv > 5000:
            cost = holdv_should - holdv
            n = math.floor(cost*.998/price/100)*100
            balance -= n*price*1.002
            holdn += n
            msg = f'buy: {n}*{price}'

        if  holdv -holdv_should  > 5000:
            cost = holdv - holdv_should
            n = math.floor(cost*.998/price/100)*100
            balance += n*price*.9985
            holdn -= n
            msg = f'sell: {n}*{price}'
        total = holdn*price + balance
        print(f'{priceInfo["trade_date"]}:b={balance}, holdn={holdn}, price={price}, t={total} msg:{msg}')
        pre_priceInfo = priceInfo


if __name__ == "__main__":
    main()