from db.conn import cursor, insertBatch, insertUpdate
from datetime import datetime, date, timedelta
import pandas as pd
from functools import wraps
from conf import conf
import json


def getKey(key, expire=86400):
    err = cursor.execute("select v,time,type from keyvdb where key=%s", [key,])
    res = cursor.fetchone()
    if res and (datetime.now() - res["time"]).total_seconds() < expire:
        if res["type"] == 'pandas':
            v = pd.read_json(res['v'])
        else:
            v = json.loads(res["v"])
        return 0, v
    return 1, "" # 过期


def addKey(key, v):
    vType = 'json'
    if isinstance(v, pd.core.frame.DataFrame):
        v = v.to_json()
        vType = 'pandas'
    else:
        v = json.dumps(v)
    cursor.insertUpdate("keyvdb", {
        "key": key, 
        "v": v, 
        "type": vType, 
        "time": datetime.now(),
    }, "key")


def clearDb():
    cursor.execute("delete from keyvdb")


"""
永久存储型Cache
"""
def withCache(pre="profit", expire=864000):
    def singleOp(func):
        @wraps(func)
        def _func(*args, **kw):
            v = None
            key = pre + ":" + ":".join(map(str, args))
            if len(kw):
                key += str(kw)
            if conf.REFRESH:
                errno = -1
            else:
                errno, v = getKey(key, expire)
            # errno = 1 过期
            if errno:
                v = func(*args, **kw)
                addKey(key, v)
                # if type(v)!=type(None):
                # else:
                #     raise Exception("Api None")
            return v

        return _func

    return singleOp
