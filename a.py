import sys,os
import pandas as pd
import backtrader as bt
from datetime import datetime
from db.conn import getProApi
from backtrader.feeds import PandasData  # 用于扩展DataFeed

from lib import logger
data_path = './tmp'
tsPro = getProApi()
start_date='20210101'
end_date='20220430'

def fetchall():
    codes = ['300750', '300059', '300661', '300390', '300122', '300769', '300274', '300316', '300073', '300124']
    codes = ['300750', '300059', '300661']
    codes = [code+'.SZ' for code in codes]
    dfs = []
    for code in codes:
        df = fetchdata(code)
        dfs.append(df)
    return dfs

def fetchdata(ts_code=''):
    global tsPro
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    csv_name = f'{str(ts_code)}_{start_date}.csv'
    csv_path = os.path.join(data_path,csv_name)
    if os.path.exists(csv_path):
        # df = pd.read_csv(csv_path)
        df = pd.read_csv(csv_path,   skiprows=0,  header=0,  )# 不忽略行 # 列头在0行
    else:
        df = tsPro.pro_bar(api=tsPro, ts_code=ts_code, start_date=start_date, end_date=end_date,adj='qfq')
        if not df.empty:
            convert_datetime = lambda x: pd.to_datetime(str(x))
            df['trade_date'] = df['trade_date'].apply(convert_datetime)
            # df.index = pd.DatetimeIndex(df.index)
            columns = {'trade_date':'datetime'}
            df = df.rename(columns=columns)
            df = df[::-1]
            df.to_csv(csv_path, index=False)
        else:
            raise f"{ts_code} has no data"

    # features=['open','high','low','close','vol','trade_date']
    # convert_datetime = lambda x:datetime.strptime(x,'%Y%m%d')

    return df

# Create a subclass of Strategy to define the indicators and logic
class SmaCross(bt.Strategy):
    # list of parameters which are configurable for the strategy
    params = dict(
        pfast=10,  # period for the fast moving average
        pslow=30   # period for the slow moving average
    )
    def __init__(self):
        super().__init__()
        sma1 = bt.ind.SMA(period=self.p.pfast)  # fast moving average
        sma2 = bt.ind.SMA(period=self.p.pslow)  # slow moving average
        logger.log(sma1)
        self.crossover = bt.ind.CrossOver(sma1, sma2)  # crossover signal
        self.dataclose = self.data0.close
    def next(self):
        if not self.position:  # not in the market
            logger.warn("by", self.dataclose[0])
            self.order_target_size(target=1)  # enter long
            # self.buy()
            logger.log(self.position)
            # if self.crossover > 0:  # if fast crosses slow to the upside
            #     self.order_target_size(target=1)  # enter long
        elif self.crossover < 0:  # in the market & cross to the downside
            # logger.log(self.position)
            pass
            # self.order_target_size(target=0)  # close long position
            # self.close()

class Strategy_runner1:
    def __init__(self, strategy, ts_code, start_date, end_date):
        global tsPro
        self.pro = tsPro
        self.ts_code = ts_code
        self.start_date = start_date
        self.end_date = end_date
        self.start_datetime = datetime.strptime(start_date,'%Y%m%d')
        self.end_datetime = datetime.strptime(end_date,'%Y%m%d')
        df = fetchdata(self.ts_code)
        df['datetime'] = df['datetime'].apply(lambda x: pd.to_datetime(str(x)))
        self.df = df.set_index('datetime')
        self.strategy = strategy
        self.cerebro = bt.Cerebro()
        

    def run(self):
        data = bt.feeds.PandasData(dataname=self.df,                               
                                    fromdate=self.start_datetime,                               
                                    todate=self.end_datetime)
        self.cerebro.adddata(data)  # Add the data feed
        self.cerebro.addstrategy(self.strategy)  # Add the trading strategy
        self.cerebro.broker.setcash(100000.0)
        # self.cerebro.addsizer(bt.sizers.FixedSize, stake=10)
        # self.cerebro.broker.setcommission(commission=0.0)
        self.cerebro.addanalyzer(bt.analyzers.SharpeRatio,_name = 'SharpeRatio')
        self.cerebro.addanalyzer(bt.analyzers.DrawDown, _name='DW')
        self.results = self.cerebro.run()
        strat = self.results[0]
        print('Final Portfolio Value: %.2f' % self.cerebro.broker.getvalue())
        print('SR:', strat.analyzers.SharpeRatio.get_analysis())
        print('DW:', strat.analyzers.DW.get_analysis())
    
        # self.cerebro.plot(iplot=False)
        return self.results


def run1():
    ts_code='300059.SZ'
    strategy_runner = Strategy_runner1(strategy=SmaCross, ts_code=ts_code, start_date=start_date, end_date=end_date)
    print('ahui')
    results = strategy_runner.run()
    # print(results)

if __name__ == "__main__":
    run1()