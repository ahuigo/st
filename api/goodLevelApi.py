import requests
import 	random
import json,math
from db import keyvDb,profitDb,metaDb
from lib import logger,codelist,profitLib
from api import xqApi
from functools import cmp_to_key
import re
import time
import pandas as pd

dcApi = None
thsApi = requests.Session()
debug = False

headers = {
    'Cache-Control': 'max-age=0',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,th;q=0.6',
}

def getDcApi():
    global dcApi
    if not dcApi:
        dcApi = requests.Session()
        dcApi.get('http://data.eastmoney.com/report/ylyc.html')
    return dcApi

# Deprecated
@keyvDb.withCache('goodLevelApi:getYearEps', expire=86400*10)
def getYearEps(code):
    global thsApi
    user_agent= 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15'
    # 业绩预测
    url =f'http://basic.10jqka.com.cn/{code}/worth.html' 
    cookies = {
        'reviewJump': 'nojump',
        'searchGuide': 'sg',
        'usersurvey': '1',
        'searchHistory': '%5B%7B%22url%22%3A%22http%3A//basic.10jqka.com.cn/601633/index.html%22%2C%22code%22%3A%22601633%22%2C%22block%22%3A%22index.html%22%2C%22title%22%3A%22601633%u957F%u57CE%u6C7D%u8F66%22%2C%22jumptype%22%3A%22f10%22%7D%2C%7B%22url%22%3A%22http%3A//basic.10jqka.com.cn/002223/index.html%22%2C%22code%22%3A%22002223%22%2C%22block%22%3A%22index.html%22%2C%22title%22%3A%22002223%u9C7C%u8DC3%u533B%u7597%22%2C%22jumptype%22%3A%22f10%22%7D%5D',
        'v': 'AqO95unN-CCDbLQUIYTLNCU3NOxOmDfacSx7DtUA_4J5FM0aXWjHKoH8C1zm',
    }
    respText = thsApi.get(url, headers=headers, cookies=cookies).text
    #respText = respText.encode('latin-1').decode('gbk')
    try:
        time.sleep(0.4)
        l = pd.read_html(respText)
        df=l[0] 
        thisYear = min(df['年度'].values)
        nextYear = thisYear+1
        thisYearEps = df[df.年度==thisYear].iloc[0]['均值']
        nextYearEps = df[df.年度==nextYear].iloc[0]['均值']
        level = df[df.年度==thisYear].iloc[0]['预测机构数']
        if level==0:
            print("levelApi.py:54",df)
        if nextYearEps=='' or 'thisYearEps'=='':
            rateEps = -0.001
        else:
            rateEps = float(nextYearEps)/float(thisYearEps) - 1
            rateEps=round(rateEps,3)
        return rateEps,level
    except Exception as e:
        print('getYearEps:',url,respText)
        raise e

def getHighLevelStocks():
    stocks = getHighLevelStocksRaw()
    rows = []
    for stock in stocks:
        if stock['nextYearProfit']=='':
            continue
        # rateBuy
        stock['level'] = stock['rateBuy']
        stock['rateEps'] = float(stock['nextYearProfit'])/float(stock['thisYearProfit']) -1
        rows.append(stock)
    return rows

@keyvDb.withCache('goodLevelApi:highLevelStocks', expire=86400*10)
def getHighLevelStocksRaw():
    global debug
    dcApi = getDcApi()
    stocks = []
    cbi = random.randint(1100575,8100575)
    cb = f'datatable{cbi}'
    for pageNo in range(1,8):
        print('pageNo:',pageNo)
        pageSize=100
        url = f'http://reportapi.eastmoney.com/report/predic?cb=datatable{cbi}&dyCode=*&pageNo={pageNo}&pageSize={pageSize}&fields=&beginTime=2019-11-09&endTime=2020-11-10&hyCode=*&gnCode=*&marketCode=*&sort=count%2Cdesc&p=3&pageNum=3&_=1604868940265' 
        params = { }
        if debug:
            res = cb+'''({"hits":4121,"size":1,"data":[{"stockName":"长城汽车","stockCode":"601633","total":92,"rateBuy":68,"rateIncrease":24,"rateNeutral":0,"rateReduce":0,"rateSellout":0,"ratekanduo":92,"lastYearEps":"0.4977","lastYearPe":"","lastYearProfit":"4.5667799E9","thisYearEps":"0.6291","thisYearPe":"","thisYearProfit":"5.7713761E9","nextYearEps":"0.765","nextYearPe":"","nextYearProfit":"7.0211103E9","afterYearEps":"","afterYearPe":"","afterYearProfit":"","lastYearActualProfit":"4.496875E9","lastYearActualEps":"0.4901","beforeYearActualProfit":"5.2073139E9","beforeYearActualEps":"0.5675","aimPriceT":"32.6","aimPriceL":"9.0","updateTime":"2020-11-09 05:00:10.000","hyBK":"481","gnBK":["682","707","718","802","900","813","815","815","815","815","816","817","817","817","845","867","879","499","500","567","570","574","596","596","596","596","596","612"],"dyBK":"199003","market":["HU"],"total_1":23,"rateBuy_1":18,"rateIncrease_1":5,"rateNeutral_1":0,"rateReduce_1":0,"rateSellout_1":0,"total_3":58,"rateBuy_3":44,"rateIncrease_3":14,"rateNeutral_3":0,"rateReduce_3":0,"rateSellout_3":0,"total_12":170,"rateBuy_12":122,"rateIncrease_12":48,"rateNeutral_12":0,"rateReduce_12":0,"rateSellout_12":0}],"TotalPage":4121,"pageNo":1,"currentYear":2020})'''
        else:
            res = dcApi.get(url, params=params).text
        m = re.match(r''+cb+r'\((.*)\)$', res)
        if not m:
            continue
        stock = json.loads(m.group(1))['data']
        stocks.extend(stock)
        time.sleep(0.2)
    stocks.sort(key=lambda x: x['rateEps'], reverse=True)
    return stocks

# @keyvDb.withCache('good:getIndicatorByCode', expire=86400*10)
def getIndicatorByCode(code):
    code = codelist.parseCodes(code)[0]
    # meta
    metas = metaDb.getMetaByCode(code, updateLevel=False)
    if metas is None:
        raise Exception(f"meta not exists for code={code}")
        return {}

    # profit
    profit = {}
    profitDf = profitLib.pullXqProfitCode(code)
    if profitDf is not None:
        profit = profitDf.iloc[0].to_dict()
    print('metas', metas)
    print('profit', profit)
    row = {**metas, **profit}
    # level
    # rateEps,level = getYearEps(code)
    # row['rateEps'] = rateEps
    # row['level'] = level
    print(row)
    return row


def printGoodLevelStock(stocks,name=""):
    for stock in stocks:
        if stock['stockName'] == name:
            print(stock)
            break

def getGoodLevelStocks(rate=0.25):
    stocks = getHighLevelStocks()
    printGoodLevelStock(stocks)
    print(stocks[0])
    
    stocks = filter(lambda x: x['rateEps']>=rate, stocks)
    stocks = filter(lambda x: x['stockCode']!="000043", stocks)
    return stocks

if __name__=='__main__':
    data = getGoodLevelStocks()
    for stock in data:
        print(stock)
        ts_code = stock['stockCode']
        ts_code = ts_code+'.SH' if ts_code[0]=='6' else ts_code+'.SZ' 
        df = profitDb.pullXqProfitCode(ts_code)
        print('profit',df)
        print(f'{stock["stockCode"]}\t'+stock['stockName']+f":{stock['thisYearEps']}~{stock['nextYearEps']}={stock['rateEps']} buy={stock['level']}")
