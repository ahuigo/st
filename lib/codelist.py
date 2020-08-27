from db import metaDb
import re

stockListStr = """
中联重科:2300
保利地产:1000
美的集团:400
徐工机械:4000
泸州老窖:200
卫星石化:1000
乐普医疗:400
爱尔眼科:400
东方财富:1300
三一重工:1000
恒力石化:1000
中南建设:1000
片仔癀:100
通策医疗:100
中国化学:2000
中国平安:200
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
金发科技:2000
亿联网络:200
新宙邦:400
立讯精密:500
宁波华翔:800
亿纬锂能:200
光环新网:600
东方雨虹:200
中公教育:300
宁德时代:100
中国太保:300
浪潮信息:200
紫金矿业:2000
山东黄金:300
招商银行:300
牧原股份:100
长春高新:100
新易盛:200

艾迪精密
迈为股份
浙江鼎力
值得买  
顺丰控股
新华保险
中国太保
歌尔股份
紫光国微
立讯精密
生益科技
蒙娜丽莎
科顺股份
康龙化成
丽珠集团
苏博特  
龙蟒佰利
新宙邦  
天赐材料
杰瑞股份
荣盛石化
恒力石化
阳光城  
金域医学
鱼跃医疗
隆基股份
小熊电器
中联重科
恒立液压
中南建设
招商积余
齐心集团
明阳智能
祁连山  
塔牌集团
德赛西威
星宇股份
平煤股份
碧水源  
智飞生物
凯莱英  
长春高新
汇川技术
华测检测
良信电器
信捷电气
晶澳科技
天顺风能
亿纬锂能
五粮液  
山西汾酒
百润股份
东方财富
红旗连锁
宝信软件
中科创达
柏楚电子
同花顺  
科大讯飞
七一二  
广和通  
闻泰科技
光迅科技
天孚通信
新易盛  
亿联网络
中顺洁柔
盐津铺子
中炬高新
安琪酵母
千禾味业
天味食品
三全食品
洽洽食品
海大集团
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
    print("abc")
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

    notOwn = '\n'.join(ownSets-goodSets)
    print(notOwn)
    print(goodSets)


