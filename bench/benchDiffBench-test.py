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


def benchStrategy(code, period=30, balance=10e4):
    # 1.get prices
    prices = priceDb.getPricesByCode(code, 1, 1000)

    # 2.setMeanLine
    MeanLine.setMeanLine(prices, period)
    # setPegLine(prices, code)

    # 4.runStrategy
    runStrategy(prices, balance)
    return prices


if __name__ == "__main__":
    # 0. find code list & make getGood
    # 0.init
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("-code", "--code", default="002916", type=str)
    parser.add_argument("-p", "--period", default=30, type=int)
    parser.add_argument("-c", "--cx", default=30, type=int)
    parser.add_argument("-cmd", "--cmd", default="bench")
    Args = parser.parse_args()
    period = Args.period

    if Args.cmd == "bench":
        from lib import file

        code = codelist.parseCodes(Args.code)[0]
        prices = benchStrategy(code, period, balance=10e4)
        filePath = "../umi-demo/src/data/meanBench.json"
        print(filePath,)
        file.save(prices, filePath)

    elif Args.cmd == "generate":
        # if Args.code:
        #     codes = codelist.parseCodes(Args.code)
        # else:
        # codes = list(codelist.stockListMap.keys())
        createStrategy(codelist.stockListMap, period, balance=10e4)
    else:
        quit("wrong cmd")
    print("end")

