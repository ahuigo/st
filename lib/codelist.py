from db import metaDb
from db import profitDb
from lib import profitLib
from api import xqApi
from lib import logger
import re
import time

stockListStr = """
# dfcf:me
中南建设:1000
中联重科:2300
浪潮信息:200
鱼跃医疗:600
科大讯飞:300
杰瑞股份:300
沪电股份:700
立讯精密:650
中顺洁柔:1100
洽洽食品:200
凯莱英:100
乐普医疗:400
新宙邦:400
东方财富:2560
碧水源:1000
光环新网:600
新易盛:200
健帆生物:200
亿联网络:400
卓胜微:100 
值得买:100
三一重工:1100
招商银行:300
保利地产:1000
恒力石化:1000
山东黄金:420
恒生电子:260
隆基股份:700
中国化学:2000
中国平安:100
新华保险:300
紫金矿业:2000
兆易创新:100

# dfcf:yht
利民股份:600
浙江鼎力:100
晶澳科技:500

# hb:yht
祁连山:600
闻泰科技:100
山西汾酒:100
恒立液压:200
昭衍新药:100
大参林:100
龙马环卫:500
宁水集团:300
益丰药房:100
恒逸石化:900
新希望:300
天康生物:600
中环股份:500
正邦科技:500
东方雨虹:200
三七互娱:300
姚记科技:300
利民股份:700
小熊电器:100
新宝股份:300

#hb:me
生益科技:400
精工钢构:1800
闻泰科技:100
新城控股:300
中海油服:800
中科曙光:300
日月股份:500
禾丰牧业:700
宁水集团:300
能科股份:300
苏博特:400
中兴通讯:300
丽珠集团:200
阳光城:1500
招商积余:400
天顺风能:1500
利民股份:600
深南电路:100
美亚柏科:600
科斯伍德:500
天孚通信:400
万孚生物:100
温氏股份:500
佳发教育:600
拓斯达:300
科顺股份:500
新媒股份:100
广和通:200
#条件单:中科创达
#条件单:智飞生物

#lihuaying
华泰证券:500
高能环境:700
华友钴业:300
克来机电:300
北方华创:100
蒙娜丽莎:300

#mother
恒立液压:100
三七互娱:300
新宝股份:300
中南建设:1100

中国中免
片仔癀
"""
def parseNames(ignore_list_str):
    names = []
    for line in ignore_list_str.split('\n'):
        m = re.match(r"\s*#\s*(?P<name>\w+)", line)#.groupdict()
        if m:
            names.append(m.groupdict()['name'])
    return names
ignore_list = parseNames(stockListStr)
#print(ignore_list)


min_change = 21

# codeStr = 'code1,code2'
def parseCodes(codeStr):
    import re

    ts_codes = codeStr.strip()
    codes = []
    names = []
    if len(ts_codes) == 0:
        return codes
    if ".txt" in ts_codes:
        ts_codes = open(ts_codes).read().strip()
    for ts_code in re.split(r"[,，\s]+", ts_codes):
        if re.match(r"\d{6}", ts_code):
            if "." not in ts_code:
                ts_code += ".SH" if ts_code[0] == "6" else ".SZ"
            codes.append(ts_code)
        elif ts_code:
            names.append(ts_code)
    if names: 
        codesNames = metaDb.getCodesByNames(names)
        codes = codes+list(codesNames.values())
    return codes

keji_codes = []

def getStockList(stockListStr):
    stockList = {}
    for line in stockListStr.strip().split("\n"):
        line = line.strip()

        m1 = re.match(r"(?P<name>\w+):(?P<num>\d+)?", line)
        m2 = re.match(r"(?P<name>\w+)", line)
        m = m1 or m2
        if m:
            m = m.groupdict()
        else:
            '''works'''
            continue

        lock = 'lock' in line
        code = metaDb.getCodeByName(m["name"])
        if not code:
            continue
        num = int(m.get("num") or 0)
        if m['name'] in ignore_list and num==0:
            continue
        if code not in stockList or num >0:
            stockList[code] = {
                "name": m["name"],
                "code": code,
                "num": num,
                "lock": lock,
            }
    return stockList

stockListMap = getStockList(stockListStr)

if __name__ == '__main__':
    ownSets = set()
    goodSets = set()
    min_level = 25

    for line in stockListStr.strip().split("\n"):
        line = line.strip()
        m1 = re.match(r"(?P<name>\w+):(?P<num>\d+)", line)
        m2 = re.match(r"(?P<name>\w+)", line)
        if m1:
            m = m1.groupdict()
            ownSets.add(m['name'])
        elif m2:
            m = m2.groupdict()
            goodSets.add(m['name'])

    badSets = ownSets-goodSets
    print("dny>1.2:")
    badMsgs = []
    for name in badSets:
        code = metaDb.getCodeByName(name)
        profit = profitDb.getProfitByCode(code)
        if not profit:
            print(name,code)
            profitLib.pullProfitCode(code)
            profit = profitDb.getProfitByCode(code)
        levelInfo = metaDb.getMetaByCode(code)
        end_date = profit["end_date"].strftime('%Y%m%d')
        msg = f"{name}\t{code}:{profit['end_date']},dny={profit['dny']},level:{levelInfo['level']}"
        
        if profit['dny']<1.20:#or levelInfo['level']<min_level:
            badMsgs.append(msg)
        else:
            logger.lg(msg)
        if not (end_date == '20200630'):
            print(f'pull code {code}')
            df = profitLib.pullXqProfitCode(code, True)
            
    print("bad or min_level:")
    for msg in badMsgs:
        logger.lg(msg, hcolor="red")
    print("good:")
    print(goodSets)


