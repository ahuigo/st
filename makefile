code?=''
sql:
	psql -U role1 ahuigo

benchMeanAsValue:
	python -u bench/benchMeanAsValue.py
benchMeanAsValue:
	python bench/benchLevelAsValue.py -c 1:200 --hold 36:39 

mean: #求均值
	python lib/MeanLine.py $(code)
migrate: 
	python db/migrate.py
init: genlist  pullProfit
genlist: 
	python db/run.py -cmd genlist
pullProfit: # cache 1day
	python db/run.py -cmd pullProfit
pullProfitGood: # cache 1day
	python db/run.py -cmd pullProfitGood


pullPrice:
	python db/run.py -cmd pullPrice -code ${code}

cleanKv:
	python db/run.py -cmd clearKv
cleanPrice:
	echo 'delete from prices' | psql  -U role1 ahuigo

strategy:
	python lib/strategyRun.py -cmd level 
getGood: # 获取好股
	@echo "get good！"
	# get good
	python db/run.py -raw -cmd getGood

show:
	python db/run.py -cmd show -code $(code)
getName:
	python db/run.py -cmd getName -code $(code)
