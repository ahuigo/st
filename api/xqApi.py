import requests,pickle
import json
from db import keyvDb
from conf.conf import DEBUG
from datetime import date,datetime
import time
import pandas as pd
import random
import os,re
from lib import logger
from api import apiUtil 

xqApi = None

def rmb(s:str)->float:
    s= s.replace('元','')
    return float(re.sub(r',','',s))

headers = {
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/11.1.2 Safari/605.1.15',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-User': '?1',
    'Sec-Fetch-Dest': 'document',
    'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7,th;q=0.6',
}
def getXqApi():
    global xqApi
    if not xqApi:
        xqApi = requests.Session()
        cookieDir = "./tmp"
        cookieFile = f"{cookieDir}/xq.cookie"
        if not os.path.exists(cookieDir):
            os.mkdir(cookieDir)
        if os.path.exists(cookieFile):
            with open(cookieFile,'rb') as f:
                xqApi.cookies.update(pickle.load(f))
        else:
            xqApi.get('https://xueqiu.com/snowman/S/SZ002185/detail', headers=headers)
            with open(cookieFile, 'wb') as f:
                pickle.dump(xqApi.cookies, f)

    return xqApi

@keyvDb.withCache('xq:getProfits', expire=86400*10)
def getProfits(ts_code):
    time.sleep(1)
    symbol = parseCode(ts_code)
    timestamp = str(int(time.time()*1000))
    url = f'https://stock.xueqiu.com/v5/stock/finance/cn/indicator.json?symbol={symbol}&type=all&is_detail=true&count=10&timestamp={timestamp}'
    print(f"pull xq indicator:{url} ... ")
    response = getXqApi().get(url, headers=headers)
    try:
        res = response.json()
    except Exception as e:
        print(response.text)
        raise e
    # if res['data']['last_report_name'] != '2020中报':
    rows = []
    for item in res['data']['list']:
        d = datetime.fromtimestamp(item['report_date']/1000)
        end_date = date(d.year,d.month,d.day)
        row = {
            'end_date': end_date,
            'code': ts_code,
            "dtprofit": item['net_profit_after_nrgal_atsolc'][0],
            "tr": item['total_revenue'][0],
            "try": item['total_revenue'][1],
        }
        logger.lg(row)
        rows.append(row)
    df =  pd.DataFrame(rows)
    df = apiUtil.add_q_value(df, 'dtprofit','dny')
    df = apiUtil.add_q_value(df, 'tr','try')
    return df


def parseCode(ts_code):
    if ts_code[-3] != ".":
        quit(f"Wrong ts_code:{ts_code}")
    code = ts_code[:-3]
    symbol = ts_code[-2:].upper() + code
    return symbol

if __name__ == '__main__':
    # 帝欧家居(SZ:002798)
    rtn = getProfits('002798.SZ')
    print(rtn)
