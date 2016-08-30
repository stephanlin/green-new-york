#!/bin/bash
PYTHON_FILE=$1
INPUT=${@:2:$#-4}
OTHERS=(${@:$#-2})
OUTPUT=${OTHERS[@]:0:1}
LOCAL_OUTPUT=${OTHERS[@]:1:1}
NUM_EXECS=${OTHERS[@]:2:1}
NAME="BDM PROJ"

HD=hadoop
SP=spark-submit

$HD fs -rm -r -skipTrash $OUTPUT
$SP --name "$NAME" --num-executors $NUM_EXECS --files hdfs:///user/sgupta04/block-groups-polygons-simple.geojson,hdfs:///data/share/bdm/hw_final/boroughs.geojson $PYTHON_FILE $INPUT $OUTPUT
rm -f $LOCAL_OUTPUT
$HD fs -cat $OUTPUT/part*  | tr -d '() ' > $LOCAL_OUTPUT
