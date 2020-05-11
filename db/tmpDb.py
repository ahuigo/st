from db.conn import cursor
from datetime import datetime,date,timedelta

tableName = 'mo_users'

def add(row):
    cursor.insertUpdate(tableName, row, onConflictKeys='username')

def addBatch(rows):
    index = ''
    err = cursor.insertBatch(tableName, rows, index)
    # err = cursor.insertBatch("tableName", profit_list, '')
    return err

def getUsers():
    cursor.execute(f"select * from {tableName}")
    rows = cursor.fetchall()
    if rows:
        return dict(rows)

def getOneByCond(end_date):
    cursor.execute("select * from tableName where end_date=%s", end_date)
    row = cursor.fetchone()
    if row:
        return row

def getListByCond(code, page=1,size=20):
    offset = (page-1)*size
    end_date = date.today()+timedelta(days=-7)
    cursor.execute("select*from tableName where end_date>=%s and code=%s limit %s offset %s", [end_date,code,size,offset])
    return cursor.fetchall()
