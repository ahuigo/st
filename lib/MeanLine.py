from datetime import datetime, date, timedelta
from dateutil.parser import parse as strptime
from db import priceDb, profitDb


"""
获取虚拟价格（如周末这种情况）
@return pos, price
prices[pos].day <= day < prices[pos+1].day 
pos==-1, 默认历史价格为第一天价格
"""


def getVirtualPriceByDay(prices, day, i=0, j=0, priceKey="close"):
    if j == 0:
        j = len(prices) - 1
    if not i < j:
        return None, None  # 不可能
    if day < prices[0]["trade_date"]:
        return -1, prices[0][priceKey]
    if prices[j]["trade_date"] == day:
        return j, prices[j][priceKey]
    if prices[j]["trade_date"] < day:
        return None, None  # 可能出现
    while i + 1 < j:
        m = (i + j) // 2
        if prices[m]["trade_date"] < day:
            i = m
        elif prices[m]["trade_date"] > day:
            j = m
        else:
            i = m
            break

    if prices[i]["trade_date"] == day:
        return i, prices[i][priceKey]
    else:
        price1 = prices[i][priceKey]
        price2 = prices[i + 1].get(priceKey, price1)
        day1 = prices[i]["trade_date"]
        day2 = prices[i + 1]["trade_date"]
        return i, price1 + (price2 - price1) * (day - day1).days / (day2 - day1).days


"""
价格累加
不包含start_day这一天的价格
@return total_price, err
"""


def calTotalPrice(prices, start_date, end_date):
    start_pos, start_price = getVirtualPriceByDay(prices, start_date)
    end_pos, end_price = getVirtualPriceByDay(prices, end_date)
    if not start_price or not end_price:
        return 0,f"no price 90days later,code={prices[0]['code']},start_date={start_date},end_date={end_date},end_price={end_price}"
    total_price = 0
    if start_pos == end_pos:
        total_price += (start_price + end_price) / 2 * (
            (end_date - start_date).days + 1
        ) - start_price
    else:
        pre_date = start_date
        pre_price = start_price
        for i in range(start_pos + 1, end_pos + 1):
            priceInfo = prices[i]
            cur_date = priceInfo["trade_date"]
            cur_price = priceInfo["close"]
            total_price += (pre_price + cur_price) / 2 * (
                (cur_date - pre_date).days + 1
            ) - pre_price
            pre_date, pre_price = cur_date, cur_price
        if pre_date != end_date:
            total_price += (pre_price + end_price) / 2 * (
                (end_date - pre_date).days + 1
            ) - pre_price
    return total_price, 0


"""
均线
"""


def setMeanLine(prices, mean_period=30):
    priceInfo = prices[0]
    priceInfo["mean"] = priceInfo["close"]
    pre_mean = priceInfo["mean"]
    pre_date = priceInfo["trade_date"]

    prices_len = len(prices)
    for j in range(1, prices_len):
        price_j = prices[j]
        cur_date = price_j["trade_date"]
        # pre:date & mean
        pre_date = prices[j - 1]["trade_date"]
        pre_mean = prices[j - 1]["mean"]
        diff_days = (cur_date - pre_date).days
        # add
        addTotalPrice, err1 = calTotalPrice(prices, pre_date, cur_date)
        delTotalPrice, err2 = calTotalPrice(
            prices,
            pre_date - timedelta(days=mean_period),
            cur_date - timedelta(days=mean_period),
        )
        # print(cur_date, addTotalPrice, delTotalPrice)
        if err2:
            # 1.no 90day before
            quit(f"err2={err2}")
        if err1:
            # 1.no 90day after
            quit(f"someerr:err1={err1},err2={err2}")
            break
        else:
            price_j["mean"] = (
                pre_mean * mean_period + addTotalPrice - delTotalPrice
            ) / mean_period


def setEvalueByMean(prices,):
    for priceInfo in prices:
        evalue = priceInfo["mean"]
        priceInfo["evalue"] = evalue
        priceInfo["change"] = 100 * priceInfo["close"] / evalue - 100


def setEvalueYesterday(prices):
    priceInfo = prices[0]
    priceInfo["mean"] = priceInfo["close"]
    pre_close = priceInfo["close"]

    prices_len = len(prices)
    for j in range(1, prices_len):
        priceInfo = prices[j]
        priceInfo["mean"] = pre_close
        pre_close = priceInfo["close"]


def setPegLine(prices, code, mean_period=15):
    return
    DIFF_PERIOD = 270
    for priceInfo in prices:
        cur_date = priceInfo["trade_date"]
        old_price1 = getVirtualPriceByDay(
            prices,
            cur_date - timedelta(days=mean_period + DIFF_PERIOD),
            priceKey="mean",
        )[1]
        old_price2 = getVirtualPriceByDay(
            prices, cur_date - timedelta(days=mean_period), priceKey="mean"
        )[1]
        # peg = profitDb.getPegByCodeDay(code, cur_date)
        # priceInfo["add"] = (old_price2 - old_price1) * mean_period / DIFF_PERIOD
        priceInfo["period"] = mean_period
        priceInfo["evalue"] = (
            old_price2 + (old_price2 - old_price1) * mean_period / DIFF_PERIOD
        )


if __name__ == "__main__":
    import json
    import datetime
    import sys
    from db import metaDb

    code = sys.argv[1] or "000988.SZ"
    code = metaDb.getCode(code)
    prices = priceDb.getPricesByCode(code, 1, 1000)
    setMeanLine(prices)
    setPegLine(prices, code)
    # for priceInfo in prices: print(priceInfo["trade_date"], priceInfo["close"], priceInfo.get("mean", 0))

    from lib import file

    filePath = '../umi-demo/src/data/mean-line.json'
    print(filePath)
    file.save(prices, filePath)
