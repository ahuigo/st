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
华新水泥:700
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
创业慧康:700
千方科技:700
金发科技:2000
亿联网络:200
新宙邦:400
立讯精密:500
宁波华翔:800
亿纬锂能:200
光环新网:600
万孚生物:200
东方雨虹
珀莱雅
中公教育
宁德时代

#杰瑞股份
#五粮液

洽洽食品
中航沈飞
华测检测
华熙生物
中信特钢
恒立液压
小熊电器
迈瑞医疗
牧原股份
荣盛发展
卓胜微
汇顶科技
领益智造
深南电路
联美控股
南极电商
捷佳伟创
浪潮信息

# 价高
# 长春高新
# 中科创达
# 同花顺
# 泰格医药
# 利安隆
#宁波华翔
# 金科股份
#阳光城
# 古井贡酒
#华夏幸福
#贵州茅台
#山西汾酒
#中南建设
#比音勒芬
#冀东水泥
##关注:
#金科股份
#中信特钢

#供热:
#联美控股
#地产pe=5
#荣盛发展
# 小米：
# 新宝股份

#高估
# 久远银海
#华测检测
#南极电商
##亏损：
#领益智造
#
#高估：
#古井贡酒
#中航沈飞
#同花顺

#财报不全:
#小熊电器
#华熙生物

#peg下降：
#科锐国际
#健盛集团

#peg<20%
#中材科技

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


min_change = 20

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
