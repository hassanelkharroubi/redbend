#!/bin/bash
. $HOME/.bash_profile
export EXPDATE=`date '+%Y%m%d_%H%M%S'`
export CVIMSLIB=$CVIMSHOME/refresh/config

export EXECPATH=$CVIMSHOME/refresh/redbend/Operations_AMQ
export WRITEPATH=$CVIMSHOME/stage/RBops
export TOINGEST=$CVIMSHOME/stage/RBops/ToIngest/
export REGION=$1
export INPUT=$WRITEPATH'/OPS_'$REGION$EXPDATE'.csv'
export OUTPUT=$WRITEPATH'/OPS_'$REGION$EXPDATE'crypt.csv'

if [ ! -d "$WRITEPATH" ]; then
  mkdir -p $WRITEPATH
fi

echo "------------------"
echo "export goes to $INPUT"

echo "`date '+%Y%m%d_%H%M%S'` start extraction RB operation "
jython $EXECPATH/getOperations_test02.py  $REGION   $INPUT

mv $INPUT $TOINGEST
echo "export is available to ingest on $TOINGEST"

echo "`date '+%Y%m%d_%H%M%S'` End extraction RB operations "
