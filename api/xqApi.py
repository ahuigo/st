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
        cookieFile = "./tmp/xq.cookie"
        if os.path.exists(cookieFile):
            with open(cookieFile,'rb') as f:
                xqApi.cookies.update(pickle.load(f))
        else:
            xqApi.get('https://xueqiu.com/snowman/S/SZ002185/detail', headers=headers)
            with open(cookieFile, 'wb') as f:
                pickle.dump(xqApi.cookies, f)

    return xqApi

def getProfits(ts_code):
    print(f"pull xq:{ts_code} ... ")
    time.sleep(1)
    symbol = parseCode(ts_code)
    timestamp = str(int(time.time()*1000))
    url = f'https://stock.xueqiu.com/v5/stock/finance/cn/indicator.json?symbol={symbol}&type=all&is_detail=true&count=8&timestamp={timestamp}'
    response = getXqApi().get(url, headers=headers)
    try:
        res = response.json()
    except Exception as e:
        print(response.text)
        raise e
    if res['data']['last_report_name'] != '2020中报':
        print("no 中报(不更新)")
        return
    rows = []
    for item in res['data']['list']:
        end_date = datetime.fromtimestamp(item['report_date']/1000).strftime('%Y%m%d')
        row = {
            'end_date': end_date,
            'ann_date': end_date,
            'code': ts_code,
            "dtprofit": item['net_profit_after_nrgal_atsolc'][0],
        }
        rows.append(row)
    df =  pd.DataFrame(rows)
    df = apiUtil.add_q_value(df, 'dtprofit','dny')
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
