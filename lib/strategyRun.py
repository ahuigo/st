from lib import logger
from lib.strategyBase import (
    calc_buy,calc_sell,calc_total,debug_stockList,createStrategy,execStrategy,
    print_unhold,
)
import os

def setLevelChange(stockListMap, price_key='price'):
    from api import sinaApi
    from db import metaDb
    codePriceMap = sinaApi.getPriceInfoByCodes(stockListMap.keys())
    for code,stockInfo in stockListMap.items():
        price = codePriceMap[code][price_key]
        metaInfo = metaDb.getMetaByCode(code, updateLevel=True)
        # metaInfo = sinaApi.getLevel(code)
        level_price = float(metaInfo['level_price'])
        if not level_price:
            logger.lg('No level_price',metaInfo,stockInfo)
            level_price = 0.9
        stockInfo['close'] = price
        stockInfo['level_price'] = level_price
        stockInfo['level'] = metaInfo['level']
        stockInfo['change'] = 100*price/level_price-100
    return


    # 2.getMeanLine+change
    from lib import MeanLine

    for code, prices in all_prices.items():
        if len(prices) == 0:
            quit(f'{code},{stockListMap[code]["name"]} has no prices')
        MeanLine.setMeanLine(prices,period)
        MeanLine.setEvalueByMean(prices,)
        stockListMap[code]["change"] = prices[-1]["change"]
        stockListMap[code]["close"] = prices[-1]["close"]
        # if DEBUG:
        #     all_prices[code] = prices[0:2]
        # todo: MeanLine.setEvalueLine(prices, code)


def setMeanChange(all_prices, stockListMap, period=15):
    # 2.getMeanLine+change
    from lib import MeanLine

    for code, prices in all_prices.items():
        if len(prices) == 0:
            quit(f'{code},{stockListMap[code]["name"]} has no prices')
        MeanLine.setMeanLine(prices,period)
        MeanLine.setEvalueByMean(prices,)
        stockListMap[code]["change"] = prices[-1]["change"]
        stockListMap[code]["close"] = prices[-1]["close"]
        # if DEBUG:
        #     all_prices[code] = prices[0:2]
        # todo: MeanLine.setEvalueLine(prices, code)

if __name__ == "__main__":
    from lib.codelist import stockListMap,min_change
    from db import metaDb,priceDb
    from datetime import date
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("-cmd", default="level") # level, mean
    parser.add_argument("-yestclose", action="store_true") # level, mean
    Args = parser.parse_args()

    # 0. init
    print_unhold(stockListMap)
    price_key = 'yestclose' if Args.yestclose else 'price'
    balance = 20e4
    max_hold_n = 60
    etf_total = 14140
    period = 30
    print(f"before:period={period} balance={balance} max_hold_n={max_hold_n},change={min_change}")

    # 1.get codelist+price
    codes = list(stockListMap.keys())

    # 2. setChange
    if Args.cmd == 'level':
        setLevelChange(stockListMap,price_key=price_key)
    elif Args.cmd == 'mean':
        all_prices = priceDb.getPullPricesByCodeList(codes)
        setMeanChange(all_prices, stockListMap, period)
    else:
        quit(f'Bad cmd')
    hold_total = sum( r["num"]* r["close"] for r in stockListMap.values()) + etf_total
    total = balance + hold_total
    print(f'hold_total={hold_total},total={hold_total+balance}')

    # 4. strategy
    stockList = createStrategy(
        stockListMap.values(), balance, max_hold_n, min_change=min_change
    )
    balance = execStrategy(stockList, balance)

    # 4. print strategy
    metaDb.patchMetaInfo(stockList)
    output = debug_stockList(stockList)
    hold_total = sum(calc_sell(r["code"], r["num"], r["close"]) for r in stockList) + etf_total
    total = balance + hold_total
    ave = total/max_hold_n
    stats = f'total:{total}, hold_total={hold_total},balance={balance},ave={ave}\n'
    logFilePath = 'log/strategy-'+str(date.today())+'.s'
    with open(logFilePath,'w') as f:
        f.write(logFilePath+'\n')
        print(stats)
        f.write(stats)
        f.write(output)
        if os.path.islink('s.log'):
            os.remove('s.log')
        os.symlink(logFilePath, 's.log')

