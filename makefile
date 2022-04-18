code?=''
sql:
	psql -U role1 ahuigo
sql2:
	psql -U ahui ahuigo

benchMeanAsValue:
	python2 -u bench/benchMeanAsValue.py
benchLevelAsValue:
	python3 bench/benchLevelAsValue.py -c 1:200 --hold 36:39 

mean: #求均值
	python3 lib/MeanLine.py $(code)
migrate: 
	python3 db/migrate.py
initDB:
	echo 'create database ahuigo; create user role1;'| psql postgres
init: migrate genlist  
genlist: 
	python3 db/run.py -cmd genlist
pullProfit: # cache 1day
	python3 db/run.py -cmd pullProfit
getGood: # 获取好股
	@echo "get good！"
	# get good
	# make getGood code=长春高新 
	python3 db/run.py -raw -cmd getGood -code ${code}
pullProfitGood: # cache 1day
	python3 db/run.py -cmd pullProfitGood


pullPrice:
	python3 db/run.py -cmd pullPrice -code ${code}

cleanKv:
	python3 db/run.py -cmd clearKv
cleanPrice:
	echo 'delete from prices' | psql  -U role1 ahuigo

strategy:
	python3 lib/strategyRun.py -cmd level 

# 查看详情
show:
	python3 db/run.py -cmd show -code $(code)
getName:
	python3 db/run.py -cmd getName -code $(code)


profile:
	python3 -m cProfile -o out.pstats db/run.py -cmd show -code 三一重工
	python3 bench/py-perf.py

