from db.conn import cursor, conn

sql_dict = {
    # "prices1":'drop table prices,keyvdb,profits',
    # "prices1":'drop table keyvdb',
    # "drop": "drop table metas,preprofits,profits,keyvdb",
    # "view":"create view profits_late as select distinct on (code) * from profits order by code,end_date desc;",
    # alter table keyvdb add type varchar(10)
    "drop": "drop table profits",
    "t": """
    create table t(
        code char(9) not null,
        label real not null
    );
    """,
    "prices": """
    create table prices(
        code char(9) not null,
        trade_date date not null,
        close   decimal(6,2) not null, -- alter table prices alter column  close type decimal(6,2);
        high   real default 0, 
        low   real default 0, 
        -- mean    decimal(7,3) default 0,      --180日均线
        -- value   decimal(6,2) default 0,      --估值
        unique (code,trade_date)
    );
    """,
    "keyvdb": """
    create table keyvdb(
        key varchar(200) not null,
        v text,
        type varchar(10),
        time timestamp not null,
        unique (key)
    );
    """,
    "metas": """
    create table metas(
        code char(9) not null,
        name varchar(10)  not null default '',
        industry varchar(10) not null default '', -- 行业
        price  decimal(6,2) not null default 0,
        -- daily_basic
        total_share int not null default 0, -- 股本数
        float_share	int not null default 0, -- 股本数
        free_share	int not null default 0, -- 股本数
        -- level
        level SMALLINT not null default 0, -- 评级
        level_price decimal(10,2) not null default 0, -- 评级
        p_change decimal(10,2), -- 变化率%, 
        update_time timestamp not null default '20170707',
        unique (code)
    );
    """,
    "preprofits": """
    create table preprofits(
        code char(9) not null,
        end_date date not null, -- 季报结束日期
        -- ann_date date not null default '19700101', -- 财报报告期
        ann_done smallint not null default 0, -- 是否更新了财报
        unique (code)
    );
    """,
    "profits": """
    create table profits(
        code char(9) not null,  -- 季报
        end_date date not null, -- 季报结束日期
        -- 营收
        tr decimal(14,2) not null default 0, 

        -- 利润（年累计）
        dtprofit  decimal(14,2) not null default 0, -- 扣非净利debut 

        -- 利润(季度)
        q_dtprofit	decimal(14,2) not null default 0, --扣非净利

        -- 利润增长
        -- tr_yoy decimal(14,2) not null default 0, --营收增长
        -- netprofit_yoy decimal(14,2) not null default 0, --净利增长
        -- q_netprofit_yoy decimal(14,2) not null default 0, --净利增长
        dtprofit_yoy decimal(14,2) not null default 0, --扣非净利增长
        

        -- 平均增长
        -- 营收: operating receipt, turnover
        try decimal(14,2) not null default 0, -- 总营收增长率%(年化), 
        dny decimal(14,2) not null default 0, -- 扣非利润增长倍数(年化1)
        pe smallint not null default 999,
        peg decimal(14,3) not null default 0, --年化价值增长倍数(年化1)
        buy smallint not null default 0,
        unique (code,end_date)
    );
    """,
}

for sql in sql_dict.values():
    try:
        print(sql)
        err = cursor.execute(sql)
        # conn.commit()
    except Exception as err:
        print(err)

cursor.execute(
    """SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'"""
)
a = cursor.fetchall()
print(a)
