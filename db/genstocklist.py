import requests
from subprocess  import getoutput
import json
for i in range(1, 118):
    url = "curl 'https://xueqiu.com/stock/screener/screen.json?category=SH&exchange=&areacode=&indcode=&orderby=symbol&order=desc&current=ALL&pct=ALL&page=2&fmc=1_17309.10&_=1521003164507' -H 'Cookie: device_id=31a499e24d37ee58b318eff98810e4be; s=fr12i6pimq; bid=f5d8ce4329316bd0beaa6018217f79a0_jd02iwgh; xq_a_token.sig=aaTVFAX9sVcWtOiu-5L8dL-p40k; xq_r_token.sig=rEvIjgpbifr6Q_Cxwx7bjvarJG0; __utmc=1; __utmz=1.1520767746.48.6.utmcsr=google|utmccn=(organic)|utmcmd=organic|utmctr=(not%20provided); Hm_lvt_1db88642e346389874251b5a1eded6e3=1518481129,1520759173,1520767744,1520998128; __utma=1.750760695.1517221536.1520997328.1521003042.51; __utmt=1; xq_a_token=5b9ca894ac0c09cf3635398f87998b70f8dcc256; xqat=5b9ca894ac0c09cf3635398f87998b70f8dcc256; xq_r_token=6c01183a4a46911a49a91170c12f91c676a4fdcc; xq_token_expire=Sun%20Apr%2008%202018%2012%3A51%3A41%20GMT%2B0800%20(CST); xq_is_login=1; u=7442994883; __utmb=1.3.10.1521003042; Hm_lpvt_1db88642e346389874251b5a1eded6e3=1521003130' -H 'Accept-Encoding: gzip, deflate, br' -H 'Accept-Language: zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7' -H 'User-Agent: Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36' -H 'Accept: application/json, text/javascript, */*; q=0.01' -H 'Referer: https://xueqiu.com/hq/screener' -H 'X-Requested-With: XMLHttpRequest' -H 'Connection: keep-alive' -H 'cache-control: no-cache' --compressed".replace('&page=', '&page={0}&').replace('curl ', 'curl -s ').format(i)
    r = getoutput(url)
    l = json.loads(r)['list']
    for d in l:
        #print(','.join(str(i) for i in [d['symbol'][2:],d['name'],d['nig']['20170930'], d['fmc']]))
        print(','.join(str(i) for i in [d['symbol'][2:],d['name'],d['fmc']]))
        quit()
