from db import metaDb, priceDb
from api import sinaApi
from conf.conf import DEBUG
from conf import conf
from collections import OrderedDict
import re, os
import math
from lib import logger


"""
佣金: 0.06%, 买卖各0.03%, 最低5元, 5/1w*2=0.1%(0.05+0.05)
印花税：0.1%(卖的时候才收取，此为国家税收，全国统一)。0.1%(0+0.1)
过户费: 每1000股1元, 最低1元(深证不收) 买卖各收取一次, 2元到20元：0.1% ~0.01%(0.05-0.005)
"""


def calc_buy(code, num, close):
    cost = num * close
    if code == "rmb":
        return cost
    commission = cost * 0.0003
    commission = 5 if commission < 5 else round(commission, 2)
    if code[0] == "6":
        # 过户
        cost += math.ceil(num / 1000)
    # logger.log(code=code,commission=commission+pass_fee,buy='buy',num=num,close=close)
    return round(cost + commission, 2)


"""
calc_sell
"""


def calc_sell(code, num, close):
    cost = num * close
    if code == "rmb":
        return cost
    # 佣金
    commission = cost * 0.0003
    commission = 5 if commission < 5 else round(commission, 2)
    # 印花税 stamp_tax = 0
    stamp_tax = round(cost * 0.001, 2)
    if code[0] == "6":
        # 过户
        cost -= math.ceil(num / 1000)
    # logger.log(code=code,commission=commission,stamp_tax=stamp_tax)
    return round(cost - commission - stamp_tax, 2)


def calc_total(stockList, balance):
    return balance + sum(calc_sell(r["code"], r["num"], r["close"]) for r in stockList)


def debug_stockList(stockList):
    buy_rate = 1
    sell_rate = 1
    output = f'buy_rate:{buy_rate},sell_rate:{sell_rate}\n'
    print(output.rstrip())
    for s in sorted(stockList, key=lambda item: item["change"]):
        s["change"] = round(s["change"], 2)
        price = s['close']
        name = s["name"]
        industry = s["industry"]
        extra = f"level_price={s.get('level_price',0)},level={s.get('level',0)},change={s['change']}"
        prefix = f"{name}{s['code']}{industry}:\t"
        msg = f"{prefix} {price}*{s['num']}({extra}) {s['strategy']}{s.get('i','')}"
        
        if s["strategy"] == "sell":
            msg = f"{prefix} {round(price*sell_rate,2)}*{s['sellNum']}({extra}) -> {s['strategy']}({s['i']})"
            msg = logger.log(msg, color="ok", call=stripPublish)
        elif s["strategy"] == "buy":
            msg = f"{prefix} {round(price*buy_rate,2)}*{s['buyNum']}({extra}) -> {s['strategy']}({s['i']})"
            msg = logger.log(msg, color="red",call=stripPublish)
        elif conf.CODE == s["code"] or s.get('level',0)<50:
            msg = logger.log(msg, color="warn",call=stripPublish)
        else:
            msg = logger.log(msg, call=stripPublish)
        output += msg+'\n'
    if conf.big:
        quit("too big or small")
    return output

def stripPublish(msg):
    if conf.PUBLISH:
        msg = re.sub(r'[\d.\*]+\(level[^)]+\)','',msg)
    return msg


def createStrategy(
    stockListOld, balance, max_hold_n=2, min_change=2.5, debug=False, step=0,
):
    BUY_RATE = 1.001
    SELL_RATE = 0.998
    total_money = (
        sum([item["num"] * item["close"] * 0.998 for item in stockListOld]) + balance
    )
    avg_stock_money = 0.98 * total_money / max_hold_n

    # 1.1 init + sort
    stockListOld = sorted(stockListOld, key=lambda item: item["change"])
    stockList = [r for r in stockListOld if not r['lock']]
    for stockInfo in stockList:
        stockInfo["strategy"] = ""

    # 2. 差价sell-buy 交换
    buy_i = 0  # 代表当前未买入的最小change
    sell_i = len(stockList) - 1
    need_sell = False
    oper_i = 0
    while buy_i <= sell_i:  # continue 必须有i变化
        sellStockInfo = stockList[sell_i]
        buyStockInfo = stockList[buy_i]
        # 钱够就买入
        if balance > 0 and not need_sell:
            # buy:忽略持有、停盘情况
            if buyStockInfo["num"] > 0 or buyStockInfo["lock"]:
                buyStockInfo["strategy"] = "hold" if buyStockInfo["num"] > 0 else "lock"
                oper_i += 1
                buyStockInfo["i"] = oper_i
                buy_i += 1
                continue
            # 价格太高
            close = buyStockInfo["close"]
            n = avg_stock_money / (100 * close * BUY_RATE)
            # if buyStockInfo["name"]=='新宙邦':
            #     logger.log(buyStockInfo,avg_stock_money=avg_stock_money,n=avg_stock_money / (100 * close))
            if n < 0.7:
                buy_i += 1
                continue
            # 钱不够
            # if buyStockInfo['name']=='万科A':
            #     logger.log(buyStockInfo,n=n,close=close,avg_stock_money=avg_stock_money)
            buyNum = round(n) * 100
            if buyNum * close * BUY_RATE > balance:
                need_sell = True
                continue

            # buybuy
            cost = calc_buy(buyStockInfo["code"], buyNum, buyStockInfo["close"])
            oper_i += 1
            buyStockInfo.update(
                {"strategy": "buy", "buyNum": buyNum, "i": oper_i, "cost": cost,}
            )

            # 计算余额
            balance -= cost
            buy_i += 1
            continue

        # 2 钱不够就卖出
        # 2.1 sell:忽略未持有、停盘情况
        if sellStockInfo["num"] < 1 or sellStockInfo["lock"]:
            sellStockInfo["strategy"] = "no" if sellStockInfo["num"] <= 0 else "lock"
            oper_i += 1
            sellStockInfo["i"] = oper_i
            sell_i -= 1
            continue
        # 2.2钱不够就卖出
        if sellStockInfo["change"] - buyStockInfo["change"] > min_change:
            need_sell = False
            sellNum = sellStockInfo["num"]
            cost = calc_sell(sellStockInfo["code"], sellNum, sellStockInfo["close"])
            oper_i += 1
            sellStockInfo.update(
                {"strategy": "sell", "sellNum": sellNum, "i": oper_i, "cost": cost,}
            )

            balance += cost
            sell_i -= 1
            continue
        else:
            break

    return stockListOld

def print_unhold(stockListMap):
    print('unhold list:')
    for code,item in stockListMap.items():
        if item['num']==0:
            print(item['name'])
    

def execStrategy(stockList, balance, step=-1):
    # 4.1 sell
    for row in [r for r in stockList if r["strategy"] == "sell"]:
        row["num"] -= row["sellNum"]
        balance += row["cost"]
    # 4.2 buy
    for row in [r for r in stockList if r["strategy"] == "buy"]:
        row["num"] += row["buyNum"]
        balance -= row["cost"]

    return balance