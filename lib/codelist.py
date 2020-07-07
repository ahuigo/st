from db import metaDb
import re

stockListStr = """
中联重科:2300
保利地产:1000
美的集团:400
徐工机械:4000
泸州老窖:200
卫星石化:1000
周大生:800
乐普医疗:400
爱尔眼科:400
东方财富:1300
三一重工:1000
恒力石化:1000
片仔癀:100
通策医疗:100
中国化学:2000
中国平安:200
智飞生物:300
新华保险:300
中顺洁柔:1100
精工钢构:4900
帝欧家居:700
恒生电子:200
光威复材:300
隆基股份:700
沪电股份:700
凯莱英:100
创业慧康:700
千方科技:700
金发科技:2000
亿联网络:200
新宙邦:400
立讯精密:500
宁波华翔:800
亿纬锂能:200
光环新网:600
东方雨虹:200
珀莱雅:100
中公教育:300
宁德时代:100
中国太保:300
浪潮信息:200
万孚生物
捷佳伟创

三七互娱 
新华保险 
中国太保 
歌尔股份 
立讯精密 
卓胜微   
杰瑞股份 
恒力石化 
隆基股份 
中南建设 
长春高新 
亿纬锂能 
山西汾酒 
东方财富 
红旗连锁 
宝信软件 
中顺洁柔 
洽洽食品 
紫金矿业 
山东黄金 

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
