#!/bin/bash
. $HOME/.bash_profile

ME=`basename $0`

REGION=$1

if [ "$REGION" = "" ]
then
        echo "$ME missing REGION"
        exit 1
fi

PS=`ps -fu $LOGNAME | grep $ME | grep -v grep | grep -v " $$ " | grep $REGION`
if [ ! "$PS" = "" ]
then
        echo "WARN: Daemon $ME already running...leaving !"
        exit 1
fi

export loop_script=$CVIMSHOME/refresh/redbend/Operations_AMQ
export CVIMSLIB=$CVIMSHOME/refresh/config

export RBCRED_EU=$CVIMSHOME/refresh/config/RB_EU.key
#export RBCRED_RU=$CVIMSHOME/refresh/config/RB_RU.key
export RBCRED_KR=$CVIMSHOME/refresh/config/RB_KR.key


if [ "$REGION" = "EU" ]
then
	python3 $CVIMSHOME/refresh/config/getRBCred.py  $REGION  $RBCRED_EU
elif [ "$REGION" = "KR" ]
then
	python3 $CVIMSHOME/refresh/config/getRBCred.py  $REGION  $RBCRED_KR
fi


if [ ! -d "$loop_script" ]; then
  mkdir -p $loop_script
fi

while true
do
  TODAY=`date '+%Y%m%d'`
        touch  $loop_script/lastwatch_RBoper
        $loop_script/extractOperations.sh $REGION 
       sleep  10 #10 sec

done
