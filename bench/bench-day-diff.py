import sys,os
from db import priceDb
from db.conn import getProApi
from datetime import datetime,timedelta
from collections import deque

#from bench.prices import prices
maxTransactionAmount = 2*10**4
deltaDiff = 0.001
# todo: buy with diff-0.01(high,low)

pro = getProApi()
# print("pro....")
# data = pro.daily(ts_code='000001.SZ', start_date='20180701', end_date='20180718')
#data = getProApi().stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')
codes = [
'300059.SZ', # 东方 0.020
#'600036.SH', # 0.016
#'300750.SZ', # 宁德
#'300498.SZ',
# '300124.SZ',
# '300769.SZ',
# '300763.SZ',
# '300316.SZ',
# '300751.SZ',
# '300760.SZ',
# '300896.SZ',
]
import json
prices = priceDb.getPullPricesByCode(codes[0],1, 500)
if 'high' not in prices[-1]:
    prices.pop()
prices = [{"trade_date": str(p['trade_date']), "close":p['close'], "high":p['high'],"low":p['low']} for p in prices]
#print(json.dumps(prices)); quit()



balance = 10**6
mysts = deque([]) #[{price, amount}] desc
trade_num = 0

class Stock:
    def __init__(self, price, amount, diff):
        self.price = price
        self.amount= amount
        self.diff= diff

'''
配对迭代: 滑动窗口
'''
import math
def iterReduce(arr, return_end=False):
    item = next(arr, None)
    if item is None:
        return

    while True:
        old_item = item
        item = next(arr, None)
        if item is None:
            if return_end:
                yield old_item, None
            break
        yield old_item,item

def iterWindow2(seq, n=2):
    for i in range(len(seq) - n + 1):
        yield seq[i: i + n]
            
def calcYj(p):
    r = p*0.00025
    r = r if r > 5 else 5
    if r<=5:
        print("r:",r)
        quit()
    return r

def sellSt(price):
    global mysts,balance,trade_num
    if len(mysts)==0:
        printSts(price)
        print("not enough sell!")
    amount = 0
    while len(mysts)>0:
        st = mysts.pop()
        print("sell diff:", strFloat(st.diff*100),",st.price:",st.price,",real.diff:",strFloat(price/st.price*100-100))
        if st.price*(1+st.diff) < price:
            amount+=st.amount
            continue
        else:
            mysts.append(st)
        break
    if amount>0:
        print("amount=",amount*100)
        trade_num += 1
        transactionAmount = amount*100*price
        balance+=amount*100*price*(0.999-0.0002) - calcYj(transactionAmount)

def buySt(price, diff):
    global mysts,balance,trade_num
    if balance>maxTransactionAmount:
        trade_num += 1
        amount = math.ceil(maxTransactionAmount/(price*100))
        transactionAmount = amount*100*price
        balance -= amount*100*price*(1+0.0001)+calcYj(transactionAmount)
        print("amount=",amount*100)
        mysts.append(Stock(price,amount,diff))
    else:
        quit("not enough balance", balance)


def total(price):
    global mysts,balance
    n = balance
    for st in mysts:
        n+=st.amount*price*100
    return '{:.1f}'.format(n)

def strFloat(f):
    return '{:.1f}'.format(f)

        
def printSts(price):
    global mysts,balance
    print("total:", total(price), end=",")
    print("balance:", strFloat(balance),end=",")
    print("mysts:", end="[")
    for st in mysts:
        print(f'{st.amount*100}*{st.price}',end=" ")
    print("]")


minBalance =balance 
import sys
if len(sys.argv)>=2:
    triggerDiff = float(sys.argv[1])
else:
    triggerDiff = 0.02

#def benchDiff( prices, ):
for p1,p2,p3 in iterWindow2(prices,3):
    if minBalance>balance:
        minBalance =balance 
    tdate, pc1, pc2= p2['trade_date'],p1['close'],p2['close']
    low,high = p3['low'],p3['high']
    diff = pc2/pc1-1
    print("")
    print(tdate, pc2, '{:.1f}'.format(diff*100))
    if diff>0.01: # not important
        price = round(pc2*(1+deltaDiff), 2)
        price = pc2
        high = pc2
        if price <=high:
            #if price < low: price = low-0
            print("sell:", end="")
            sellSt(price)
    elif diff< -triggerDiff: # < 0.02
        price = round(pc2*(1-deltaDiff),2)
        price = pc2
        low = pc2
        if price>=low:
            #if price > high: price = high+0
            print("buy:",end="")
            buySt(price, -diff)
    if abs(diff)>0.01:
        printSts(pc2)
    else:
        print("total:", total(pc2))

print("totalend:", total(prices[-1]['close']), ",trade_num:", trade_num, ",minBalance:", minBalance)
print("tirggerDiff", triggerDiff)
