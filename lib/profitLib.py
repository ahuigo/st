from conf import conf
from lib import logger
from api import xqApi
from db import profitDb, keyvDb
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

def pullProfitCode(ts_code):
    return pullXqProfitCode(ts_code)

@keyvDb.withCache("pullXqProfitCode", 86400 * 30)
def pullXqProfitCode(ts_code, debug=False):
    df = xqApi.getProfits(ts_code)
    logger.lg(df)
    if isinstance(df, type(None)):
        return
    if df.empty:
        return

    df = df[
        "code,end_date,dtprofit,q_dtprofit,dny,tr,try".split(",")
    ]
    df["peg"] = df['dny']
    df["buy"] = 1
    df_length = len(df)
    for index, row in df.iterrows():
        # 有些数据index not exists
        if (index+4) not in df.index:
            continue
        # 季度净得增长
        if index + 4 < df_length:
            # 利润list
            monotonical_num = 0
            q_dtprofit_list = df.loc[index : index + 3, "q_dtprofit"].to_list()

            # 递减数
            for q_dtprofit1, q_dtprofit2 in iterWindow(q_dtprofit_list, 2):
                if q_dtprofit2<=0 or q_dtprofit1 < q_dtprofit2 *.8:
                    monotonical_num += 1
            # 递减太多
            if ( monotonical_num >= 3):
                df.loc[index, "buy"] = 0

    profitCols = set(df.columns) - set(["float_share", "free_share"])
    df = df[profitCols]
    if debug:
        print(df)
    profitDb.addProfitBatch(df[profitCols])
    return df

if __name__ == "__main__":
    code = '002798.SZ'
    code = '000333.SZ'
    pullXqProfitCode(code, True)
    df = profitDb.getProfitByCode(code)
    print(dict(df))
