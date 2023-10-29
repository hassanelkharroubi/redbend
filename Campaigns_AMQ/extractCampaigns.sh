#!/bin/bash
. $HOME/.bash_profile
export EXPDATE=`date '+%Y%m%d_%H%M%S'` 
export CVIMSLIB=$CVIMSHOME/refresh/config

export EXECPATH=$CVIMSHOME/refresh/redbend/Campaigns_AMQ
export CTLPATH=$CVIMSHOME/ctl/redbend/Campaigns_AMQ
export WRITEPATH=$CVIMSHOME/stage/RBcampaigns/
export TOINGEST=$CVIMSHOME/stage/RBcampaigns/ToIngest/
export REGION=$1 


if [ ! -d "$WRITEPATH" ]; then
  mkdir -p $WRITEPATH
fi
if [ ! -d "$CTLPATH" ]; then
  mkdir -p $CTLPATH
fi
if [ ! -d "$EXECPATH" ]; then
  mkdir -p $EXECPATH
fi
echo "------------------"
echo "export goes to $INPUT"

echo "`date '+%Y%m%d_%H%M%S'` start extraction RB campaign "
jython get_campaigns.py  $REGION $WRITEPATH $TOINGEST
mv $INPUT $TOINGEST
echo "export is available to ingest on $TOINGEST"

echo "`date '+%Y%m%d_%H%M%S'` End extraction RB campaign "
