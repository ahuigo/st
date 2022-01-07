import requests
import json
from db import keyvDb
from conf.conf import DEBUG
from datetime import date
import pandas as pd
import random
import os,re
from lib import logger
import math

def add_q_value(df, profit_indicator, yoy_key):
    df_len = len(df)
    qk = 'q_'+profit_indicator
    df[qk] = 0
    df[yoy_key] = float(0)

    for i in range(df_len):
        curr = df.iloc[i]
        end_date = curr['end_date']
        end_date = end_date.strftime('%Y%m%d')
        # // .strftime('%Y%m%d')
        j = i+1
        if j==df_len:
            if end_date[-4:] == '0331':
                profit_dedt = curr[profit_indicator]
            elif end_date[-4:] == '0630':
                profit_dedt = (curr[profit_indicator]/2)
            elif end_date[-4:] == '0930':
                profit_dedt = (curr[profit_indicator]/3)
            elif end_date[-4:] == '1231':
                profit_dedt = (curr[profit_indicator]/4)
            else:
                quit(f'wrong enddate:{end_date}')
        else:
            prev = df.iloc[j]
            if end_date[-4:] == '0331':
                profit_dedt = curr[profit_indicator]
            else:
                # print(curr, int(curr[profit_indicator]))
                profit_dedt = curr[profit_indicator]-prev[profit_indicator]
        if not math.isnan(profit_dedt):
            df[qk].iat[i] = (profit_dedt)
        # except Exception as e:
        #     logger.log(qk,profit_dedt, df)
        # df.iloc[i]['q_profit_dedt'] = int(profit_dedt)
    if len(df)>=8:
        l = df[qk]
        dny = (sum(l[0:4])/sum(l[4:8]))
        if sum(l[0:4])<=0:
            dny = -1
        df[yoy_key].iat[0] = dny
    return df