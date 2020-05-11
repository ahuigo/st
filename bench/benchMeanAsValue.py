from db.conn import cursor
from db import priceDb
from datetime import datetime, date, timedelta
import math
from copy import deepcopy
from lib import MeanLine
from lib import logger
import sys, os
import json
from lib.strategyBase import (
    execStrategy,
    createStrategy,
    calc_sell,
    calc_buy,
    calc_total,
    debug_stockList,
)
from conf.conf import DEBUG
from conf import conf


BALANCE = 20 * 1e4
max_hold_n = 10
BUY_RATE = 1.001
SELL_RATE = 0.998

# 交易开始时间
start_trade_date = date(2016, 6, 5)
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


def gen_all_prices(codes):
    cursor.execute(
        "select code,trade_date,close as close from prices where code=any(%s) order by code,trade_date",
        [codes],
    )
    for row in cursor:
        trade_date = row["trade_date"]
        code = row["code"]
        priceInfo = {**row, "close": float(row["close"])}
        all_prices[trade_date][row["code"]] = priceInfo
        all_stocks[code].append(priceInfo)
    # gen_all_prices_mean(all_prices, mean_period)


def gen_all_prices_mean(mean_period=15):
    for code, prices in all_stocks.items():
        # MeanLine.setEvalueLine(prices)
        MeanLine.setMeanLine(prices, mean_period=mean_period)


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
    logger.lg({"code": code, "date": cur_date})
    quit("hahah")


def exec_trade(trade_date, stockList, balance, min_change=1, step=0):
    # 1. get state: stocklist, close
    # code:, close:,change:, state: null/hold/lock, strategy:buy/sell/null, num:23
    prices = all_prices.get(trade_date, {})
    # stockList = [r for r in stockList if r["num"] > 0 or r["close"] < 180]
    if len(prices) == 0:
        return stockList, balance

    for row in stockList:
        code = row["code"]
        row["strategy"] = ""

        if code in prices:
            close = float(prices[code]["close"])
            oprice = prices[code]["mean"]
            # oprice = getMeanPriceByCode(code, trade_date)
            row["change"] = 100 * close / oprice - 100
            row["close"] = close
            row["lock"] = 0
        else:
            if code != "rmb":
                # 可忽略
                # row["close"] = yesterday_price[code]
                row["lock"] = 1

    createStrategy(stockList, balance, max_hold_n, min_change, step=step)
    balance = execStrategy(stockList, balance, step=step)

    if conf.min_step <= step <= conf.max_step or conf.big:
        print("step:", step)
        debug_stockList(stockList)
        print("New end balance", balance, "\n")
    return stockList, balance


def get_codes():
    global start_trade_date
    cursor.execute(
        # "select code from metas where name=ANY(%s)", [good_code_names],
        'select distinct on(code) code from prices limit 20',
    )
    all_codes = [r["code"] for r in cursor]

    for i in range(3):
        t_trade_date = start_trade_date - timedelta(days=i)
        cursor.execute(
            "select distinct on(code) code from prices where trade_date=%s",
            [t_trade_date],
        )
        codes = [r["code"] for r in cursor]
        if codes:
            start_trade_date = t_trade_date
            codes = list(set(codes) & set(all_codes))
            break

    rm_codes = {
        "600519.SH",
    }
    codes = list(set(codes) - rm_codes)
    logger.lg(
        "len(codes)=", len(codes),
    )
    return codes


import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-c", "--change", default=0, type=int)
parser.add_argument("-p", "--period", default="5:20", type=str)
parser.add_argument("--dstep", default="9:8", type=str)
parser.add_argument("-d", "--debug", action="store_true")
Args = parser.parse_args()

if __name__ == "__main__":
    min_change = Args.change
    change_range = range(0, 50) if min_change == 0 else [min_change]
    [conf.min_step, conf.max_step] = list(map(int, Args.dstep.split(":")))
    [start_period, end_period] = list(map(int, Args.period.split(":")))

    codes = get_codes()
    gen_all_prices(codes)

    # print(codes)
    stockList = priceDb.getPricesByDate(start_trade_date, codes)
    ostock_list = deepcopy(stockList)
    output = []

    for period in range(start_period, end_period, 1):
        gen_all_prices_mean(period)
        for min_change in change_range:
            balance = BALANCE
            stockList = deepcopy(ostock_list)
            for i in range(0, trade_day_num, 1):
                [stockList, balance] = exec_trade(
                    start_trade_date + timedelta(days=i),
                    stockList,
                    balance,
                    min_change,
                    step=i,
                )
                if conf.Dtrade:
                    total = calc_total(stockList, balance)
                    print(f"step{i}:change-period-total:", min_change, period, total)
                if i >= conf.max_step >= conf.min_step:
                    break

            total = calc_total(stockList, balance)
            output.append([min_change,period,total] )
            print(f"endstep({i}):change-period-total:", min_change, period, total)


    # 用mean做估值
    filePath = '../umi-demo/src/data/benchMeanAsValue.json'
    print(filePath)
    json.dump(output,open(filePath,'w') )

