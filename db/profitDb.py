from db.conn import cursor
from datetime import datetime, date, timedelta
from subprocess import getoutput


def addProfit(row):
    cursor.insertUpdate("profits", row)


def addProfitBatch(profit_list):
    # print(profit_list)
    cursor.insertBatch("profits", profit_list, "code,end_date")
    # cursor.insertBatch("profits", profit_list, '')

def updateBuy():
    sql='''
        WITH qdtVariance AS (
        select code,end_date,netprofit_yoy,
            q_dtprofit-LEAD(q_dtprofit,1) OVER(
                PARTITION BY code 
                ORDER BY end_date desc
            ) variance
        from profits
    ), latest2 AS(
        SELECT *, 
        ROW_NUMBER() OVER(PARTITION BY code ORDER BY end_date DESC) AS rk 
        FROM qdtVariance
    )
    SELECT code,end_date,netprofit_yoy,variance FROM latest2 where rk=1
    '''
    cursor.execute(sql)
    rows = []
    for row in cursor:
        row = dict(row)
        row['buy'] = 1 if row['netprofit_yoy']>7 and row['variance']>0 else 0
        row.pop('variance')
        rows.append(row)
    print(rows)
    addProfitBatch(rows)

def showCode(code):
    cmd = f"""	echo "select * from metas where code='{code}' " | psql -U role1 ahuigo;"""
    print(getoutput(cmd))
    cmd = f"""	echo "select * from profits where code='{code}' order by end_date desc" | psql -U role1 ahuigo;"""
    print(getoutput(cmd))


def hasTradeProfit(code, end_date):
    sql = f"""select code,end_date from profits where code='{code}' and end_date='{end_date}' """
    cursor.execute(sql)
    row = cursor.fetchone()
    return True if row else False


def getProfitByCode(code,):
    sql = f""" select p.*,metas.name from (select distinct on (code) code,end_date,pe,dtprofit_yoy,peg,dny,try from profits where code='{code}' order by code,end_date desc  ) p join metas on metas.code=p.code"""
    cursor.execute(sql)
    row = cursor.fetchone()
    return row


def getProfitHistoryByPage(code, page=1, size=20):
    offset = (page - 1) * size
    end_date = date.today() + timedelta(days=-7)
    cursor.execute(
        "select*from profits where end_date>=%s and code=%s limit %s offset %s",
        [end_date, code, size, offset],
    )
    return cursor.fetchall()
