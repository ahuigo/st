#!/bin/sh
# ./grep.sh good20 good30 good60 super all
arr=('super' care good 'all' ) 
arg="$@"
#for t in "${arr[@]}";
for t in "$@";
do
    echo $t
    python gen.py -a $t --raw > "$t.txt"
    if [[ ! ${arg//$t} == $arg ]];then
        echo $t
    fi
done
