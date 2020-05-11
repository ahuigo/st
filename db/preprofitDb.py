from db.conn import cursor
from datetime import datetime,date,timedelta

def add(row):
    cursor.insertUpdate('preprofits', row)

def addBatch(rows):
    index = ''
    cursor.insertBatch("preprofits", rows, index)

def getSyncProfitList():
    today = str(date.today())
    cursor.execute("select * from preprofits where ann_date<=%s and ann_done=0", today)
    return list(cursor)
