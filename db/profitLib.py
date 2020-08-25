from conf import conf
from lib import logger
from db import profitDb, keyvDb
from db.conn import pro,ak
from api import sinaApi
from itertools import islice


def iterWindow(seq, n=2):
    it = iter(seq)
    result = tuple(islice(it, n))
    if len(result) == n:
        yield result
    for elem in it:
        result = result[1:] + (elem,)
        yield result

# 计算倍数
def calc_multiple(newV, oldV):
    if oldV<0:
        oldV = -oldV
    if newV > 0 and oldV > 0:
        return newV / oldV
    else:
        return 1


def calc_yoy(newV, oldV):
    if oldV > 0 and newV > 0:
        return 100 * newV / oldV - 100
    else:
        return 0

def singleton(cls):
    _instance = {}

    @wraps(cls)
    def _singleton(*args, **kwargs):
        ck = str(cls) + str(args) + str(kwargs)
        if ck not in _instance:
            _instance[ck] = cls(*args, **kwargs)
        return _instance[ck]

    return _singleton


# @singleton
def get_basic_from_day(ts_code, ann_date):
    basics = pro.daily_basic(
        ts_code=ts_code,
        start_date=ann_date,
        fields="ts_code,trade_date,pe_ttm,pb,float_share,free_share",
    )
    return basics


def parse_basic_by_day(df, ann_date):
    # end_date = (date.strptime('%Y%m%d',ann_date)+timedelta(days=30)).strftime('%Y%m%d')
    df1 = df[df.trade_date >= ann_date]
    if len(df1):
        return df1.iloc[-1]
    else:
        return None

@keyvDb.withCache("profitLib", 86400 * 5)
def pullProfitCode(ts_code):
    # df = pro.fina_indicator(
    #     ts_code=ts_code,
    #     start_date="20150901",
    #     fields="""
    #     ann_date,end_date,npta,profit_dedt,q_npta,q_dtprofit,tr_yoy,q_netprofit_yoy, netprofit_yoy,dt_netprofit_yoy,roe_dt
    #     """.replace(
    #         " ", ""
    #     ),
    # )
    df = ak.stock_financial_abstract(ts_code[0:6])
    df = sinaApi.tidy_sina_profits(df)
    print(df)
    # print(df.iloc[0:2,:].T)

    df.fillna(0, inplace=True)
    df["code"] = ts_code
    df = df[
        "code,ann_date,end_date,netprofit,q_netprofit,ny,tr,try".split( ",")
    ]
    df["peg"] = 1
    df["pe"] = 0
    df["buy"] = 1
    df_length = len(df)
    for index, row in df.iterrows():
        # 有些数据index not exists
        if (index+4) not in df.index:
            continue
        # 季度净得增长
        if index + 4 < df_length:
            # 排除利润下滑
            monotonical_num = 0
            q_netprofit_list = df.loc[index : index + 3, "q_netprofit"].to_list()

            # 递减的
            for q_dtprofit1, q_dtprofit2 in iterWindow(q_netprofit_list, 2):
                if q_dtprofit2<=0 or q_dtprofit1 < q_dtprofit2 *.8:
                    monotonical_num += 1
            # 递减太多
            if ( monotonical_num >= 3):
                df.loc[index, "buy"] = 0


    if conf.DEBUG:
        print(df)
    profitCols = set(df.columns) - set(["float_share", "free_share"])
    profitDb.addProfitBatch(df[profitCols])
    # return df[profitCols]
