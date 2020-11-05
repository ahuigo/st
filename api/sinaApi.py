import requests
import json
from db import keyvDb
from conf.conf import DEBUG
from datetime import date
import pandas as pd
import random
import os,re,time
from lib import logger

def rmb(s:str)->float:
    s= s.replace('元','')
    return float(re.sub(r',','',s))

def parse163Code(ts_codes):
    codes = {}
    for tsCode in ts_codes:
        code = tsCode[:6]
        type = '0' if code[0] == '6' else '1'
        code = type + code
        codes[code] = tsCode
    return codes



def getPriceInfoByCodes(ts_codes):
    return getPriceInfoByCodes163(ts_codes)
    stocks = {}
    for code in ts_codes:
        print('getPrice:',code)
        stocks[code] = {'price':getCurPriceByCode(code)}
    return stocks

def getPriceInfoByCodes163(ts_codes):
    codeListMap = parse163Code(ts_codes)
    codeList= list(codeListMap.keys())
    if len(ts_codes)==0:
        raise Exception("empty ts_codes")
    codeListStr = ','.join(codeList)
    headers = {
        "Referer": f'http://quotes.money.163.com/{codeList[0]}.html',
        "Origin": f"http://quotes.money.163.com",
    }
    callback = '_ntes_quote_callback51308036'
    url = f'http://api.money.126.net/data/feed/{codeListStr},money.api?callback={callback}'
    res = requests.get(url, headers=headers).text
    if not res or callback not in res:
        logger.lg('Api: get price error,'+codeListStr+'\n')
        quit()
    else:
        res = res.strip().rstrip(');').replace(callback+'(', '');
        res = json.loads(res)
    rtn = {}
    for code,info in res.items():
        ts_code = codeListMap[code]
        rtn[ts_code] = {
            'price':info['price'],
            'yestclose':info['yestclose'],
        }
    return rtn

def getCurPriceByCode(ts_code):
    return getCurPriceByCodeSina(ts_code)

def getCurPriceByCodeSina(ts_code="000007.SZ"):
    if DEBUG:
        return random.randint(1, 20)
    symbol = parseCode(ts_code)
    headers = {
        "Origin": f"http://quotes.sina.cn",
        "Referer": f"http://quotes.sina.cn/cn/view/finance_app_detail.php?symbol={symbol}",
    }
    url = f"http://hq.sinajs.cn/list={symbol}"
    res = requests.get(url, headers=headers).text
    if symbol not in res:
        quit(f"{symbol} price not found")

    price = float(res.split(",")[3])
    if date.today().strftime("%Y-%m-%d") not in res or not price:
        logger.lg('wrong price',date.today().strftime("%Y%m%d") , res)
        quit()
    return price


def parseCode(ts_code):
    if ts_code[-3] != ".":
        quit(f"Wrong ts_code:{ts_code}")
    code = ts_code[:-3]
    symbol = ts_code[-2:].lower() + code
    return symbol


# @keyvDb.withCache('sina:profitCode')
def getProfitsByCode_todo(ts_code="000007.SZ", yearNum=1):
    code = ts_code[:-3]
    symbol = ts_code[-2:].lower() + code
    year = {1: "one", 2: "two", 3: "three", 4: "four", 5: "five"}[yearNum]
    callback = f"{year}YearData"
    url = f"http://quotes.sina.cn/cn/api/openapi.php/CompanyFinanceService.getFinanceReportAll?paperCode={code}&frType=lrb&yearNum={yearNum}&callback={callback}"
    headers = {
        "Referer": f"http://quotes.sina.cn/cn/view/finance_app_detail.php?symbol={symbol}"
    }
    res = requests.get(url, headers=headers).text
    json_str = res.split("\n")[1][len(callback) + 1 :]
    rows = json.loads(json_str)["result"]["data"]["report_list"]
    for end_date, row in rows.items():
        ann_date = row["publish_date"]
        for item in row["data"]:
            if item["item_field"] == "NETPARESHARPROF":
                netprofit = item["item_value"]
    return json
    # return pd.read_json(json, orient=)


# @keyvDb.withCache('sina:getlevel',60)
def getLevel(ts_code="000007.SZ"):
    time.sleep(0.3)
    code = ts_code[:-3]
    symbol = ts_code[-2:].lower() + code
    url = f"http://stock.finance.sina.com.cn/stock/api/openapi.php/StockMixService.khdGetStockComment?extra=mbj,ylyc,ndpj&chwm=32040_0002&device_id_fake=250213b90b10239d&imei=355212349711234&wm=b122&device_id_spns=250213b90b10239d&version=4.7.0.2&device_id_old=250213b90b10239d&from=7047095012&deviceid=209049f05b094d65&symbol={symbol}"
    rtn = requests.get(url, timeout=10)
    #print(rtn.text) 
    res = rtn.json()
    jgpg = res["result"]["data"].get("jgpj",[])
    mbj = res["result"]["data"]["mbj"] #均价
    avg = mbj.get("avg", .1) 
    level = 0
    for jgpg3 in jgpg:
        if jgpg3['month']==3:
            level += parseLevel(jgpg3)
        elif jgpg3['month']==1:
            level += 3*parseLevel(jgpg3)
    row= {"level": level, "level_price": avg}
    return row

def parseLevel(jgpg3):
    for k in [
        "sell_counts",
        "reduce_counts",
        "neutral_counts",
        "buy_counts",
        "hold_counts",
    ]:
        jgpg3[k] = int(jgpg3[k])
    level = (
        2*jgpg3["buy_counts"]
        + jgpg3["hold_counts"]
        - 2*jgpg3["sell_counts"]
        - jgpg3["reduce_counts"]
    )
    return level


def add_q_value(df, profit_indicator, yoy_key):
    df_len = len(df)
    print(df)
    print(profit_indicator)
    df[profit_indicator] = df[profit_indicator].apply(lambda x: rmb(x))
    qk = 'q_'+profit_indicator
    df[qk] = 0

    for i in range(df_len):
        curr = df.iloc[i]
        end_date = curr['end_date']
        j = i+1
        if j==df_len:
            if end_date[-4:] == '0331':
                profit_dedt = curr[profit_indicator]
            elif end_date[-4:] == '0630':
                profit_dedt = (curr[profit_indicator]/2)
            elif end_date[-4:] == '0930':
                profit_dedt = (curr[profit_indicator]/3)
            elif end_date[-4:] == '1231':
                profit_dedt = (curr[profit_indicator]/4)
            else:
                quit(f'wrong enddate:{end_date}')
        else:
            prev = df.iloc[j]
            if end_date[-4:] == '0331':
                profit_dedt = curr[profit_indicator]
            else:
                # print(curr, int(curr[profit_indicator]))
                profit_dedt = curr[profit_indicator]-prev[profit_indicator]
        df['q_'+profit_indicator].iat[i] = (profit_dedt)
        # df.iloc[i]['q_profit_dedt'] = int(profit_dedt)
    df[yoy_key] = float(0)
    if len(df)>=8:
        l = df[qk]
        print(sum(l[0:4])/sum(l[4:8]))
        df[yoy_key].iat[0] = (sum(l[0:4])/sum(l[4:8]))
    return df

def tidy_sina_profits(df: pd.DataFrame):
    df=df.rename(columns={
        "截止日期":'end_date',
        "净利润":'netprofit',
        "主营业务收入":'tr',
    })
    df = df[['end_date','netprofit','tr']]
    df['end_date'] = df['end_date'].apply(lambda x:x.replace('-',''))
    # df['ann_date'] = df['end_date']
    df.insert(1, 'ann_date', df['end_date'])
    df = add_q_value(df, 'netprofit','ny')
    df = add_q_value(df, 'tr','try')

    # 计算基础数据pe p-e-g
    ny = df.loc[0, "ny"]
    # peg = max(1 + 1 / pe, dny) if pe > 0 else 1
    df["peg"] = float(0)
    df.loc[0, "peg"] = ny
    return df
     


# 财报
# curl -H 'Referer: http://quotes.sina.cn/cn/view/finance_app_detail.php?symbol=sh601318'   'http://quotes.sina.cn/cn/api/openapi.php/CompanyFinanceService.getFinanceReportAll?paperCode=601318&frType=lrb&yearNum=1&callback=fiveYearData'

# 评级
# curl  'http://stock.finance.sina.com.cn/stock/api/openapi.php/StockMixService.khdGetStockComment?extra=mbj,ylyc,ndpj&chwm=32040_0002&device_id_fake=250213b90b10239d&imei=355212349711234&wm=b122&device_id_spns=250213b90b10239d&version=4.7.0.2&device_id_old=250213b90b10239d&from=7047095012&deviceid=209049f05b094d65&symbol=sh600519'

if __name__ == '__main__':
    rtn = getPriceInfoByCodes163(['000858.SZ','000157.SZ','600436.SH'])
    print(rtn)
