from db import metaDb
from db import profitDb
from db import profitLib
from api import xqApi
from lib import logger
import re
import time

stockListStr = """
中联重科:2300
徐工机械:4000
长春高新:100
中南建设:1000
浪潮信息:200
鱼跃医疗:600
科大讯飞:300
立讯精密:650
中顺洁柔:1100
洽洽食品:200
中公教育:300
卫星石化:1000
凯莱英:100
乐普医疗:400
新宙邦:400
东方财富:2560
碧水源:1000
光环新网:600
新易盛:200
健帆生物:200
亿联网络:400
值得买:100
三一重工:1100
招商银行:300
保利地产:1000
恒力石化:1000
山东黄金:420
恒生电子:260
隆基股份:700
中国化学:2000
中国平安:200
新华保险:300
中国太保:300
紫金矿业:2000
兆易创新:100

生益科技:400
新城控股:300
中海油服:800
中科曙光:300
禾丰牧业:700
能科股份:600
阳光城:1300
招商积余:400
天顺风能:1500
深南电路:100
富祥药业:500
拓斯达:300
新媒股份:100

宁波华翔:800
沪电股份:700


中科曙光
浙江鼎力
克来机电
拓斯达  
宝通科技
值得买  
顺丰控股
歌尔股份
紫光国微
圣邦股份
立讯精密
生益科技
深南电路
蒙娜丽莎
科顺股份
温氏股份
康龙化成
新和成  
丽珠集团
富祥股份
苏博特  
新宙邦  
天赐材料
昊华科技
杰瑞股份
荣盛石化
恒力石化
新城控股
阳光城  
金域医学
凯普生物
鱼跃医疗
健帆生物
大参林  
益丰药房
隆基股份
韦尔股份
兆易创新
江山欧派
新宝股份
小熊电器
中联重科
恒立液压
中南建设
新媒股份
招商积余
科斯伍德
齐心集团
明阳智能
祁连山  
塔牌集团
德赛西威
星宇股份
玲珑轮胎
平煤股份
龙马环卫
玉禾田  
高能环境
碧水源  
福莱特  
智飞生物
凯莱英  
长春高新
汇川技术
华测检测
宁波水表
良信电器
信捷电气
阳光电源
晶澳科技
天顺风能
山西汾酒
中海油服
百润股份
东方财富
华泰证券
宝信软件
柏楚电子
中科创达
同花顺  
金山办公
宇信科技
长亮科技
科大讯飞
美亚柏科
能科股份
交控科技
七一二  
光迅科技
广和通  
天孚通信
新易盛  
博汇纸业
中顺洁柔
博威合金
盐津铺子
安井食品
中炬高新
安琪酵母
天味食品
三全食品
洽洽食品
禾丰牧业
紫金矿业


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
    print("not end_date or dny<1.2:")
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
        
        if profit['dny']<1.20 and end_date=='20200630':
            badMsgs.append(msg)
        else:
            logger.lg(msg)
        if not (end_date == '20200630'):
            print(f'pull code {code}')
            # dfProfit = xqApi.getProfits(code)
            df = profitLib.pullXqProfitCode(code, True)
            
    print("bad:")
    for msg in badMsgs:
        logger.lg(msg, hcolor="red")
    print("good:")
    print(goodSets)


