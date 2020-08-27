import requests
import json
from db import keyvDb
from conf.conf import DEBUG
from datetime import date,datetime
import time
import pandas as pd
import random
import os,re
from lib import logger

xqApi = None

def rmb(s:str)->float:
    s= s.replace('元','')
    return float(re.sub(r',','',s))

def getXqApi():
    global xqApi
    if not xqApi:
        cookieFile = "./tmp/xq.cookie"
        if os.path.exists(cookieFile):
            with open(cookieFile) as f:
                cookies = requests.utils.cookiejar_from_dict(pickle.load(f))
                xqApi = requests.session(cookies=cookies)
        else:
            xqApi = requests.Session()

        url='https://xueqiu.com/snowman/S/SZ002230/detail'
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

        xqApi.get('https://xueqiu.com/snowman/S/SZ002185/detail', headers=headers)

    return xqApi

def getProfits(ts_code):
    symbol = parseCode(ts_code)
    timestamp = str(int(time.time()*1000))
    url = 'https://stock.xueqiu.com/v5/stock/finance/cn/indicator.json?symbol={symbol}&type=all&is_detail=true&count=5&timestamp={timestamp}'
    res = getXqApi().get(url).json()
    rows = []
    for item in res['data']['list']:
        row = {
            'end_date': datetime.fromtimestamp(item['report_date']/1000).strftime('%Y%m%d'),
            'code': ts_code,
        }

    return data


def parseCode(ts_code):
    if ts_code[-3] != ".":
        quit(f"Wrong ts_code:{ts_code}")
    code = ts_code[:-3]
    symbol = ts_code[-2:].upper() + code
    return symbol

if __name__ == '__main__':
    rtn = getProfits('')
    print(rtn)
