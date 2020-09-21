#!/bin/bash

fails=0
dname=log-$(date +%s)
mkdir ${dname}
for ((i=1;i<400;i++))
do
  echo "now running $i times"
  go test -run 2A > ${dname}/${i} 2>&1
  if [ $? != 0 ];then ((fails++)); echo waring; mv ${dname}/${i} ${dname}/fail-${fails}; fi
done
echo ${fails}
