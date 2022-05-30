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
from datetime import datetime

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
        # print(stock)
        # rateBuy
        stock['level'] = stock['rateBuy']
        try:
            # stock['rateEps'] = float(stock['thisYearProfit'])/float(stock['lastYearActualProfit']) -1
            # stock['rateEps'] = float(stock['nextYearProfit'])/float(stock['lastYearActualProfit']) -1
            # stock['rateEps'] = float(stock['nextYearProfit'])/float(stock['thisYearProfit']) -1
            stock['rateEps'] = float(stock['EPS4'])/float(stock['EPS1']) -1
        except Exception as err:
            print(stock)
            print(err)
            raise err

        rows.append(stock)
    return rows

@keyvDb.withCache('goodLevelApi:highLevelStocks', expire=86400*10)
def getHighLevelStocksRaw():
    global debug
    dcApi = getDcApi()
    stocks = []
    cbi = random.randint(1100575,8100575)
    cb = f'datatable{cbi}'
    for pageNo in range(1,2):
        print('pageNo:',pageNo)
        # api1
        # https://reportapi.eastmoney.com/report/predic?cb=datatable8078283&dyCode=*&pageNo=1&pageSize=100&fields=&hyCode=*&gnCode=*&marketCode=*&sort=rateBuy%2Cdesc&p=3&pageNum=3&_=1604868940265
        # beginTime='2019-11-09'
        # endTime='2020-11-10'
        # url = f'http://reportapi.eastmoney.com/report/predic?cb=datatable{cbi}&dyCode=*&pageNo={pageNo}&pageSize={pageSize}&fields=&beginTime={beginTime}&endTime={endTime}&hyCode=*&gnCode=*&marketCode=*&sort=rateBuy%2Cdesc&p=1&pageNum=1&_={timestamp}' 
        # api2:
        # https://data.eastmoney.com/report/profitforecast.jshtml 股票盈利预测
        timestamp=datetime.now().timestamp()
        url = f'https://datacenter-web.eastmoney.com/api/data/v1/get?callback={cb}&reportName=RPT_WEB_RESPREDICT&columns=WEB_RESPREDICT&pageNumber={pageNo}&pageSize=50&sortTypes=-1&sortColumns=RATING_ORG_NUM&p=1&pageNo=1&pageNum=1&_={timestamp}'
        print(url)
        params = { }
        if debug:
            res = cb+'''({"data":[{"stockName":"长城汽车","stockCode":"601633","total":92,"rateBuy":68,"rateIncrease":24,"rateNeutral":0,"rateReduce":0,"rateSellout":0,"ratekanduo":92,"lastYearEps":"0.4977","lastYearPe":"","lastYearProfit":"4.5667799E9","thisYearEps":"0.6291","thisYearPe":"","thisYearProfit":"5.7713761E9","nextYearEps":"0.765","nextYearPe":"","nextYearProfit":"7.0211103E9","afterYearEps":"","afterYearPe":"","afterYearProfit":"","lastYearActualProfit":"4.496875E9","lastYearActualEps":"0.4901","beforeYearActualProfit":"5.2073139E9","beforeYearActualEps":"0.5675","aimPriceT":"32.6","aimPriceL":"9.0","updateTime":"2020-11-09 05:00:10.000","hyBK":"481","gnBK":["682","707","718","802","900","813","815","815","815","815","816","817","817","817","845","867","879","499","500","567","570","574","596","596","596","596","596","612"],"dyBK":"199003","market":["HU"],"total_1":23,"rateBuy_1":18,"rateIncrease_1":5,"rateNeutral_1":0,"rateReduce_1":0,"rateSellout_1":0,"total_3":58,"rateBuy_3":44,"rateIncrease_3":14,"rateNeutral_3":0,"rateReduce_3":0,"rateSellout_3":0,"total_12":170,"rateBuy_12":122,"rateIncrease_12":48,"rateNeutral_12":0,"rateReduce_12":0,"rateSellout_12":0}],"TotalPage":4121,"pageNo":1,"currentYear":2020})'''
            res = cb+'''({"result":{"data":[{"SECUCODE":"603605.SH","SECURITY_NAME_ABBR":"珀莱雅","RATING_ORG_NUM":48,"RATING_BUY_NUM":32,"RATING_ADD_NUM":16,"YEAR1":2021,"YEAR_MARK1":"A","EPS1":2.047229788948,"YEAR2":2022,"YEAR_MARK2":"E","EPS2":3.6661875,"YEAR3":2023,"YEAR_MARK3":"E","EPS3":4.6233125,"YEAR4":2024,"YEAR_MARK4":"E","EPS4":5.769088888889,"INDUSTRY_BOARD":"美容护理","INDUSTRY_BOARD_SZM":"M","MARKET_BOARD":"069001001001","DEC_AIMPRICEMAX":235.01,"DEC_AIMPRICEMIN":194.5,"RATING_LONG_NUM":48}]}}'''
        else:
            res = dcApi.get(url, params=params, headers=headers).text
        m = re.match(r''+cb+r'\((.*)\);$', res)
        if not m:
            continue
        stock = json.loads(m.group(1))['result']['data']
        for st in stock:
            st['stockName'] = st['SECURITY_NAME_ABBR']
            st['stockCode'] = st['SECUCODE'].split('.')[0]
            st['rateBuy'] = st['RATING_BUY_NUM']
        stocks.extend(stock)
        time.sleep(0.2)
    stocks.sort(key=lambda x: x['rateBuy'], reverse=True)
    return stocks

# @keyvDb.withCache('good:getIndicatorByCode', expire=86400*10)
def getIndicatorByCode(code):
    code = codelist.parseCodes(code)[0]
    # meta
    metas = metaDb.getMetaByCode(code, updateLevel=False)
    if metas is None:
        raise Exception(f"meta not exists for code={code}")

    # profit
    profit = {}
    profitDf = profitLib.pullXqProfitCode(code)
    if profitDf is not None:
        profit = profitDf.iloc[0].to_dict()
    # print('metas', metas)
    # print('profit', profit)
    row = {**metas, **profit}
    # level
    # rateEps,level = getYearEps(code)
    # row['rateEps'] = rateEps
    # row['level'] = level
    # print(row)
    return row


def filterGoodLevelStock(stocks,name=""):
    name = name.split(".")[0]
    for stock in stocks:
        if stock['stockName'] == name or stock["stockCode"]==name:
            import json
            logger.log("filter:",name)
            print(json.dumps(stock, ensure_ascii=False, indent=2))
            return [stock]
    return []

def getGoodLevelStocks(rate=0.25, code=''):
    stocks = getHighLevelStocks()
    if code:
        stocks = filterGoodLevelStock(stocks, code)
        return stocks
    
    print("filter rateEps>=", rate)
    stocks = list(filter(lambda x: x['rateEps']>=rate, stocks))
    # stocks = filter(lambda x: x['stockCode']!="000043", stocks)
    print("lengh good stocks:%d" % len(stocks))
    return stocks

if __name__=='__main__':
    data = getGoodLevelStocks()
    for stock in data:
        print(stock)
        ts_code = stock['stockCode']
        ts_code = ts_code+'.SH' if ts_code[0]=='6' else ts_code+'.SZ' 
        df = profitLib.pullXqProfitCode(ts_code)
        print('profit',df)
        print(f'{stock["stockCode"]}\t'+stock['stockName']+f":{stock['thisYearEps']}~{stock['nextYearEps']}={stock['rateEps']} buy={stock['level']}")
