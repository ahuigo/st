import tushare as ts
import pandas as pd
import numpy as np
from datetime import datetime
from datetime import date, timedelta
from dateutil.parser import parse as strptime
now = datetime.now()
def prev_weekday():
    adate = datetime.today()
    adate -= timedelta(days=7)
    while adate.weekday() > 4: # Mon-Fri are 0-4
        adate -= timedelta(days=1)
    return adate.strftime('%Y%m%d')

import os,sys,json
import logging
import code
import argparse
parser = argparse.ArgumentParser()
parser.add_argument('-c', '--code', default='')
parser.add_argument('-n',  default=0, help='股票数量')
parser.add_argument('-cmd', '--cmd', default='')
parser.add_argument('-ub', '--update-basic', action="store_true")
parser.add_argument('-a', '--all', default="good20")
parser.add_argument('--raw', action="store_true")
parser.add_argument('-i', '--interact', action="store_true")
parser.add_argument('-d', '--debug', action="store_true")
parser.add_argument('-r', '--refresh', action="store_true")
Args = parser.parse_args()

def interact():
    if Args.interact:
        code.interact(local=locals())

logger = logging.root
if Args.debug:
    logging.basicConfig(format='%(asctime)s:%(levelname)s:%(filename)s:%(lineno)s:%(message)s', 
        level=logging.INFO)
if Args.raw:
    pd.set_option('display.max_columns', None)  # or 1000
    pd.set_option('display.max_rows', None)  # or 1000
    pd.set_option('display.max_colwidth', -1)  # or 199
    pd.options.display.width = None

def debug(v):
    print(v)

def file_db(k,v=None, is_df=False, expire=864003650):
    file_name = f'cache/{k}'
    if v is None:
        if os.path.exists(file_name) and datetime.now().timestamp() - os.path.getctime(file_name) < expire:
            res = json.load(open(file_name))
            if is_df: res = pd.DataFrame(res)
            return True, res
        else:
            return False,None
    else:
        dirname = os.path.dirname(file_name)
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        if is_df: v = v.to_dict()
        json.dump(v, open(file_name,'w'),ensure_ascii=False)
        return True

from functools import wraps
def singleton(cls):
    _instance = {}
    @wraps(cls)
    def _singleton(*args, **kwargs):
        if cls not in _instance:
            ck = str(cls)+str(args)+str(kwargs)
            _instance[ck] = cls(*args, **kwargs)
        return _instance[ck]
    return _singleton

def file_cache(key, expire, verify_empty=True, options={}, nkey=0):
    def decorator(cls):
        @wraps(cls)
        def wrapper(*args, **kwargs):
            fkey = key
            for k in args[1:1+nkey]:
                fkey += '.'+k
            if Args.refresh:
                ok = False
            else:
                ok, value = file_db(fkey, expire=expire, **options)
            if not ok:
                value = cls(*args, **kwargs)
                if not verify_empty or value:
                    file_db(fkey, value, **options)
            return value
        return wrapper
    return decorator

'''
start api
'''
key = open('conf/ts.key').read().strip()
pro = ts.pro_api(key)

now = datetime.now()


start_date = (date.today()-timedelta(days=7)).strftime('%Y%m%d')
end_date = (date.today()-timedelta(days=1)).strftime('%Y%m%d')
ok, trade_date = file_db('trade_date', expire=46400)
if not ok:
    df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date)
    trade_date = df[df.is_open==1].iloc[-1]['cal_date']
    if not trade_date:
        raise 'Not find trade_date'
    file_db('trade_date', trade_date)
    logger.info(('trade_date:', trade_date))
print("start_date:", start_date, "trade_date", trade_date)

class Gen():
    def __init__(self):
        logger.error(['init.....', str(datetime.now())])
        self.pro = pro
        self.today = date.today().strftime('%Y%m%d')
        self.trade_date = trade_date
        self.start_date, self.end_date =self.get_end_date()
        self.disclosure = self.get_disclosure()
        self.stock_list = self.get_list()
        self.basics = self.get_basic()
        self.last_week = prev_weekday()
        logger.error('init end.....')

    def get_list(self):
        ok, data = file_db('list')
        if not ok:
            # fields='ts_code,symbol,name,area,industry,list_date')
            data = pro.query('stock_basic', exchange='', list_status='L', fields='ts_code,name,industry')
            file_db('list', data.to_dict())
        else:
            data = pd.DataFrame(data)
        return data

    def not_expire(self, file_path, expire):
        if not os.path.exists(file_path):
            return False;
        create_time = os.path.getmtime(file_path)
        return now.timestamp() - create_time < expire

    '''
    是否有新的财报
    '''
    def has_disclosure(self, my_end_date, ts_code, expire = 56400):
        dc = self.disclosure
        if not dc[dc.ts_code==ts_code].empty:
            actual_date = dc[dc.ts_code==ts_code].iloc[0].actual_date
            if actual_date is not None:
                end_date = dc[dc.ts_code==ts_code].iloc[0].end_date
                ann_time = strptime(actual_date)
                if my_end_date != end_date and now.timestamp() > ann_time.timestamp()+expire:
                    return True
        return False

    '''
    get_basic
    '''
    @singleton
    @file_cache('daily_basic', 86400, verify_empty=False, options={'is_df':True}, nkey=1)
    def get_basic(self, trade_date=''):
        if not trade_date:
            trade_date = self.trade_date
        basic = pro.daily_basic(trade_date=trade_date, fields='ts_code,total_mv,circ_mv,close,pe,pe_ttm,trade_date')
        if basic.empty:
            raise Exception('Can not get all daily_basic')
        return basic
    

    '''
    income
    '''
    @singleton
    @file_cache('income.json', 86400, verify_empty=False, nkey=1)
    def get_income(self, ts_code):
        df = pro.income(ts_code=ts_code, start_date='20170630', fields='ts_code,end_date,n_income,n_income_attr_p')
        end_date = df.iloc[0].end_date
        last_year = str(int(end_date[:4])-1)
        month_date = end_date[4:]
        income = 1
        if len(df)>4:
            income = (
                df.iloc[0].n_income 
                + df[df.end_date==last_year+'1231'].iloc[0].n_income 
                - df[df.end_date==last_year+month_date].iloc[0].n_income
            )/10000
        return income

    """
    每日指标
    """
    def get_daily_basic(self, ts_code):
        start_date = (date.today()-timedelta(days=50)).strftime('%Y%m%d')
        basic = pro.daily_basic(ts_code=ts_code, start_date=start_date, 
            fields='ts_code,total_mv,circ_mv,close,pe,pe_ttm')
        return basic

    '''
    增长率列表
    '''
    def yoy_list(self, yoys, len_df):
        return list(map(lambda x: round(x,2), yoys.tolist()))+[-1]*len_df

    """
    财务指标
    """
    def get_indicator(self, ts_code, refresh = False):
        #if not start_date: trade_date = (date.today()-timedelta(days=4)).strftime('%Y%m%d')
        file_indicator = f'cache/indicator.{ts_code}'
        resnull = pd.Series([])
        res = resnull
        if (
            os.path.exists(file_indicator) 
            and not Args.refresh and not refresh
            and self.not_expire(file_indicator, 86400*10) 
        ):
            res = pd.Series(json.load(open(file_indicator)))

            # dc = self.disclosure
            # if not dc[dc.ts_code==ts_code].empty:
            #     dc_pre_date = dc[dc.ts_code==ts_code].iloc[0].pre_date
            #     dc_end_date = dc[dc.ts_code==ts_code].iloc[0].end_date
            # else:
            #     dc_pre_date = None
            #     dc_end_date = None
            # res = res.append(pd.Series({'dc_pre_date':dc_pre_date, 'dc_end_date':dc_end_date}))
            # json.dump(res.to_dict(), open(file_indicator,'w'), ensure_ascii=False)

            if self.has_disclosure(res.end_date, ts_code):
                res = resnull
            # 每日指标
            elif  Args.update_basic and ts_code in self.basics.ts_code.values:
                basics = self.basics
                basic = basics[basics.ts_code==ts_code].iloc[0]
                res.update(basic)
                json.dump(res.to_dict(), open(file_indicator,'w'), ensure_ascii=False)

        if res.empty:
            logger.error('get indicator....'+ts_code)
            df = pro.fina_indicator(ts_code=ts_code, start_date='20170901', fields='or_yoy,end_date,netprofit_yoy,profit_dedt,dt_netprofit_yoy')
            if Args.debug: 
                debug(['indicator:'])
                debug(df)
            indicator = df.iloc[0]
            len_df = len(df)
            extra = {
                'ny': self.yoy_list(df[0:4]['netprofit_yoy'], 4-len_df),
                'dny': self.yoy_list(df[0:4]['dt_netprofit_yoy'], 4-len_df),
                'ory': self.yoy_list(df[0:4]['or_yoy'], 4-len_df), # 营业收入同比增长率
            }
            indicator = indicator.append(pd.Series(extra))
            if Args.interact: code.interact(local=locals())

            basics = self.basics
            if ts_code in basics.ts_code.values:
                basic = basics[basics.ts_code==ts_code].iloc[0]
            else:
                basics = self.get_daily_basic(ts_code)
                if basics.empty:
                    raise Exception(f'Not find daily basic of {ts_code}')
                else:
                    basic = basics.iloc[0]

            # add basic
            res = basic.append(indicator)

            # dc
            dc = self.disclosure
            if not dc[dc.ts_code==ts_code].empty:
                dc_pre_date = dc[dc.ts_code==ts_code].iloc[-1].pre_date
                dc_end_date = dc[dc.ts_code==ts_code].iloc[-1].end_date
            else:
                dc_pre_date = None
                dc_end_date = None
            res = res.append(pd.Series({'dc_pre_date':dc_pre_date, 'dc_end_date':dc_end_date}))

            # add name
            sl = self.stock_list
            names = sl[sl.ts_code==ts_code].iloc[0][['name','industry']]
            res = res.append(pd.Series(names))

            # write
            json.dump(res.to_dict(), open(file_indicator,'w'), ensure_ascii=False)

        if res.empty:
            raise Exception(f"{ts_code} stock info empty")

        # add last_week price range
        #last_basics = self.get_basic(self.last_week)
        #last_basic = last_basics[last_basics.ts_code==ts_code]
        #pr = round(100*(res.close/last_basic.iloc[0].close-1), 2) if not last_basic.empty else None
        #res = res.append(pd.Series({'pr':pr}))


        # add income
        # res = res.append(pd.Series({'n_income': self.get_income(ts_code)}))

        # add peg
        res = self.gen_peg(res)
        return res
    
    '''
    财报时间
    '''
    @singleton
    def get_disclosure(self):
        d = date.today()
        if   d.month>9: end_date = d.strftime("%Y0930")
        elif d.month>6: end_date = d.strftime("%Y0630")
        elif d.month>3: end_date = d.strftime("%Y0331")
        else: end_date = str(d.year)+"1231"
        file_name = 'disclosure.json'
        ok,df = file_db(file_name,is_df=True, expire=86400*5)
        if not ok:
            df = pro.disclosure_date(end_date=end_date)
            file_db(file_name,v=df, is_df=True)
        if Args.interact: code.interact(local=locals())
        return df

    def get_end_date(self):
        d = date.today()
        if   d.month>=9: 
            end_date = d.strftime("%Y0930")
            start_date = d.strftime("%Y0630")
        elif d.month>=6: 
            end_date = d.strftime("%Y0630")
            start_date = d.strftime("%Y0331")
        elif d.month>=3: 
            end_date = d.strftime("%Y0331")
            start_date = str((d-timedelta(days=300)).year)+"1231"
        elif d.month <3: 
            end_date = str((d-timedelta(days=300)).year)+"1231"
            start_date = str((d-timedelta(days=300)).year)+"0930"
        return start_date, end_date

    '''
    业绩预告
    '''
    @singleton
    @file_cache('forcast.json', 86400, verify_empty=False, options={'is_df':True},nkey=1)
    def get_forecasts(self, ts_code):
        logger.error('get_forecasts: '+ts_code)
        start_date , end_date = self.start_date, self.end_date
        res = pro.forecast(ts_code=ts_code, start_date=start_date, fields='ts_code,ann_date,end_date,type,p_change_min,p_change_max,net_profit_min') 
        #if res.empty: raise Exception(f'Can not get all forecast for ts_code {ts_code}')
        if not res.empty:
            res = res[0:1]
        return res

    '''
    
    '''
    def add_forecast(self, indicator):
        ts_code = indicator.ts_code
        forecasts = self.get_forecasts(ts_code)
        forecastd = forecasts[forecasts.ts_code==ts_code]
        fc = {
            'type': '未知' ,
            'p_change_min':1,
            'p_change_max':1,
        }
        if not forecastd.empty:
            fc = forecastd.iloc[0][list(fc.keys())].to_dict()
        indicator = indicator.append(pd.Series(fc))
        return indicator

    def gen_peg(self, stock):
        ts_code = stock.ts_code
        yoy = (sum(stock.dny[:4])+sum(stock.ny[:4]))/8
        if stock.pe_ttm is not None and yoy!=0:
            peg = stock.pe_ttm/yoy
        else:
            ttm = 0
            peg = 0
        extra = {
            'peg':round(peg,4),
            'myoy':round(yoy,2),
        }
        return stock.append(pd.Series(extra))
    '''
    解禁
    '''
    @file_cache('float-ratio.json', 864000, verify_empty=False, options={'is_df':False},nkey=1)
    def add_float(self, ts_code, stock):
        ts_code = stock.ts_code
        start_date = (date.today()+timedelta(days=-45)).strftime('%Y%m%d')
        end_date = (date.today()+timedelta(days=30)).strftime('%Y%m%d')
        df = pro.share_float(ts_code=ts_code, start_date=start_date, end_date=end_date)
        ratio = 0 if df.empty else round(df['float_ratio'].sum(),2)
        extra = {
            'float_ratio':ratio,
        }
        return extra
         
    '''
    快报
    '''
    def get_express(self, ts_code):
        #end_date = self.end_date()
        r = pro.express(ts_code=ts_code,start_date="20181231", fields='ts_code,ann_date,end_date,revenue,operate_profit,total_profit,n_income,total_assets')
        return r

    '''
    '''
    def add_extra(self, indicator):
        ts_code = indicator.ts_code
        indicator = self.add_forecast(indicator)

        extra = self.add_float(ts_code, indicator)
        return indicator.append(pd.Series(extra))
    


from xlparser import saveXlsx

def save2xls(df, outfile):
    rows = [df.columns.to_list()]+list(df.T.to_dict('list').values())
    saveXlsx(rows, outfile)

gen = Gen()
"""
Gen Data
"""
def main():
    if Args.n:
        n = Args.n
        print(n)
        data = gen.get_basic()[0:int(n)]
    else:
        data = gen.get_basic()

    if Args.code:
        names = Args.code.split()
        print(names)
    else:
        names = []

    good_stocks = []
    stock_list = gen.get_list()
    for k,stock_basic in data.iterrows():
        ts_code = stock_basic.ts_code
        try:
            name = stock_list.loc[stock_list['ts_code']==ts_code].iloc[0]['name']
        except Exception as e:
            print("ts_code not exited：", ts_code)
            continue
        if 'ST' in name:
            #print(f'skip ST {name}')
            continue

        if Args.code:
            if name not in names:
                continue

        try:
            indicator = gen.get_indicator(ts_code)

            indicator['float_mv'] = indicator.circ_mv/indicator.total_mv
            cond = (not indicator.empty 
                and indicator.profit_dedt > 0 \
                and indicator.myoy>20
                and indicator.end_date == '20190331'
                and indicator.total_mv > 100e4
                and indicator.float_mv> 0.8
            )
            cond_good20 = ((indicator.myoy>20 and indicator.dny[0]>20 and  indicator.pe_ttm <25) and(
                    (
                    indicator.ny[0]>20 and all(map(lambda x:x>15,indicator.dny[:4])) # 稳定性调节
                    and indicator.dny[0]> 20 and all(map(lambda x:x>15,indicator.ny[:4])) # 稳定性调节
                    and all(map(lambda x:x>10,indicator.ory[:4]))# 稳定性调节
                    ) or (
                    indicator.ny[0]>20 and all(x>y for x, y in zip(indicator.ny, indicator.ny[1:3])) # 稳定性调节
                    and indicator.dny[0]>20 and all(x>y for x, y in zip(indicator.dny, indicator.dny[1:3])) # 稳定性调节
                    and indicator.ory[0]>20 and all(x>y for x, y in zip(indicator.ory, indicator.ory[1:3])) # 稳定性调节
                    )
                ) 
                and not all(x<y for x, y in zip(indicator.ny, indicator.ny[0:4]))
                    ) 
            if False:
                print((indicator.myoy>20 and indicator.dny[0]>20 and  indicator.pe_ttm <25))
                print(indicator.ny[0]>20 and all(map(lambda x:x>16,indicator.dny[:4])))
                print(indicator.dny[0]> 20 and all(map(lambda x:x>16,indicator.ny[:4])))
                print(cond_good20,cond_good20)
            select = False
            if Args.all == 'all' :
                indicator = gen.add_extra(indicator)
                select = True
            elif name in ['美的集团','中国平安x','中国太保','招商银行','深南电路x','新野纺织x','鲁阳节能x', '国祯环保x']:
                indicator = gen.add_extra(indicator)
                select = True
            else: 
                if not ( Args.all == 'good20' and cond and cond_good20) :
                    continue

                indicator = gen.add_extra(indicator)
                
                if indicator.type not in ['预减','不确定','续亏','首亏','略减'] and indicator.float_ratio<10:
                    select = True
            if select:
                good_stocks.append(indicator)
        except Exception as e:
            logger.error((e,stock_basic.ts_code))
            if Args.debug: raise e
            break

    if len(good_stocks):
        fields = ['peg','industry','name','float_ratio','type','p_change_min','pe_ttm','ts_code','close','dny','end_date','myoy','dc_pre_date','total_mv','float_mv']
        df = pd.DataFrame(good_stocks)
        df['float_mv'] = df.circ_mv/df.total_mv
        df.total_mv = df.total_mv//10000
        df = df[fields].sort_values(by=['industry', 'peg'])
        df=df.rename(index=str, columns={
            "dny": "扣非净利增长率", 
            "dc_pre_date": "财报披露时间", 
        })

        #print(df)
        #df.to_csv(Args.all+'.csv')
        debug(df)
    else:
        quit('empty')

def single():
    import re
    ts_code = Args.code
    if re.match(r'\d{6}',ts_code):
        if '.' not in ts_code:
            ts_code += '.SH' if ts_code[0]=='6' else '.SZ'
    elif '.HK' in ts_code:
        pass
    else:
        data = gen.get_list()
        ts_code = data.loc[data['name']==ts_code].iloc[0].ts_code
    r = gen.get_indicator(ts_code)
    # 披露
    print('账务披露')
    dis = gen.get_disclosure()
    print(dis[dis.ts_code==ts_code])

    # 预告
    print('业绩预告')
    fc = gen.get_forecasts(ts_code)
    print(fc[fc.ts_code==ts_code])
    # 业绩快报
    # print('快报')
    # express = gen.get_express(ts_code) 
    # print(express)
    # print('分红')
    #d = pro.dividend(ts_code='600848.SH', end_date="20181231", fields='ts_code,div_proc,stk_div,record_date,ex_date')
    # print(d)
    print(r)

if __name__ == '__main__':
    if Args.code and len(Args.code.split())==1:
        single()
    elif Args.cmd:
        exec(Args.cmd)
    else:
        main()

