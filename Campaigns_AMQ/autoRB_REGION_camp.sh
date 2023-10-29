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
        echo "WARN: Daemon $ME $REGION already running...leaving !"
        exit 1
fi

export loop_script=$CVIMSHOME/refresh/redbend/Campaigns_AMQ
export CVIMSLIB=$CVIMSHOME/refresh/config

export RBCRED_EU=$CVIMSHOME/refresh/config/RB_EU.key
export RBCRED_KR=$CVIMSHOME/refresh/config/RB_KR.key
export RBCRED_RU=$CVIMSHOME/refresh/config/RB_RU.key

if [ "$REGION" = "EU" ]
then
	python3 $CVIMSHOME/refresh/config/getRBCred.py  $REGION  $RBCRED_EU
elif [ "$REGION" = "KR" ]
then
	python3 $CVIMSHOME/refresh/config/getRBCred.py  $REGION  $RBCRED_KR
elif [ "$REGION" = "RU" ]
then
	python3 $CVIMSHOME/refresh/config/getRBCred.py  $REGION  $RBCRED_RU
fi

while true
do
       ${CVIMSHOME}/checkLock.sh
       if [ $? -eq 0 ]
       then
               TODAY=`date '+%Y%m%d'`
               touch  $loop_script/lastwatch_RBcamp
               $loop_script/extractCampaigns_V2.sh $REGION
       fi

       sleep  10 #1min

done
