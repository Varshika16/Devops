!#/bin/sh
java -jar my-app
a=ps -ef | grep jarfile| wc -l
if a>=1
wait 3 mins
py test.py
b=ps -ef | grep python| wc -l
if b>=1
wait 5 min
sh shell_test.sh.txt
