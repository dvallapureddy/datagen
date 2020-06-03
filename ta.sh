NUM_ITERATIONS=$1
#GOLDEN_SET_DIR=$2
#WORK_DIR=$3
#MyVF=$4
WATERLINEDATA_HOME=/home/amrutha/kentfp_auto/waterlinedata/
GOLDEN_SET_DIR=/home/amrutha/tagdatagen/datagen/snapshot_goldendataset
WORK_DIR=/home/amrutha/tagdatagen/datagen/results
MyVF=mrkr
#SEEDS=~/test/BOdemoNew.csv
SNAP_PREF=R
COMP_PREF=C
# example ./tag_verify 7 /media/sf_leon/TagAutomation/goldenSetKern /media/sf_leon/TagAutomation/RunResults

# waterlinedata/bin/waterline utils com.waterlinedata.tag_utils.TFASnapshot -directory  /media/sf_leon/TagAutomation/RunResults4/R6
# waterlinedata/bin/waterline utils com.waterlinedata.tag_utils.TFAComparator -oldDirectory /media/sf_leon/TagAutomation/goldenSetKern  -newDirectory /media/sf_leon/TagAutomation/RunResults4/R6  -resultDirectory  /media/sf_leon/TagAutomation/RunResults4/c6
# waterlinedata/bin/waterline utils com.waterlinedata.tag_utils.TFAResultProcessor  -resultDirectory  /media/sf_leon/TagAutomation/RunResults4 -subDirPrefix c -subDirVisuals AveragePercentage1


if [ -d "$WORK_DIR" ]
then
    echo "Directory "$WORK_DIR" exist. Should cleanup to continue. 
    echo !!!!!! ARE YOU SURE YOU WANT TO REMOVE THE CONTENT OF THE DIRECTORY "$WORK_DIR""
read -p "Cleanup directory content (y/n)?" choice
case "$choice" in 
  y|Y ) 
	echo "yes"
	rm -r "$WORK_DIR"/* 
	;;
  n|N ) echo "no, cannot continue, existing"; exit 0;;
  * ) echo "cannot continue, existing"; exit 0;;
esac
fi
echo start processing 
#$WATERLINEDATA_HOME/agent/bin/waterline format  -virtualFolder tesst --master local
#$WATERLINEDATA_HOME/agent/bin/waterline schema  -virtualFolder tesst --master local
#$WATERLINEDATA_HOME/agent/bin/waterline profile  -virtualFolder tesst --master local
$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.FixTags -domain p -enable false
$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.TFAPopulate -root /home/amrutha/mrkr -file /home/amrutha/tagdatagen/datagen/metadata.csv -seedsOnly true>text.log


#Assume we have empty profiled repository + populated seeds
######## 
#$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.FixTagAssociations -file $SEEDS 
## tag first time
#$WATERLINEDATA_HOME/agent/bin/waterline tag  -virtualFolder $MyVF --executor-memory 2g --driver-memory 2g  --num-executors 2 -incremental false --master local
## first snapshot
#$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.TFASnapshot -directory  $WORK_DIR/"$SNAP_PREF"1
## compare with golden(benchmark) set
#$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.TFAComparator -oldDirectory $GOLDEN_SET_DIR  -newDirectory $WORK_DIR/"$SNAP_PREF"1 -resultDirectory  $WORK_DIR/"$COMP_PREF"1
for ((i=2;i<=NUM_ITERATIONS+1;i++)); do
#  echo === $i ==========================================================================================
# echo --------   generate accepted and rejected  -----------------------------------------------------------------
$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.GenerateRAScript -directory $GOLDEN_SET_DIR -output $WORK_DIR/rej_accept$i.scv -nRejected 1 -nAccepted 1
# echo ----------- simulate accepted/rejected --------------------------------------------------------------
$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.FixTagAssociations -file $WORK_DIR/rej_accept$i.scv
# echo ------------ run tag discovery -------------------------------------------------------------
$WATERLINEDATA_HOME/agent/bin/waterline tag  -virtualFolder $MyVF --executor-memory 2g --driver-memory 2g  --num-executors 2 -incremental false --master local
# echo ------------- result snapshot -----------------------------------------------------------
$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.TFASnapshot -directory  $WORK_DIR/$SNAP_PREF$i
# echo -------------- compare result with benchmark  -----------------------------------------------------------
$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.TFAComparator -oldDirectory $GOLDEN_SET_DIR  -newDirectory $WORK_DIR/$SNAP_PREF$i  -resultDirectory  $WORK_DIR/$COMP_PREF$i


done
#echo --------------- generate JPEG files with individual summary and averare % summary 
$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.TFAResultProcessor  -resultDirectory  $WORK_DIR -subDirPrefix $COMP_PREF -subDirVisuals AveragePercentage
#$WATERLINEDATA_HOME/agent/bin/waterline utils com.waterlinedata.tag_utils.TFAResultProcessor  -resultDirectory  $WORK_DIR -subDirPrefix $COMP_PREF -subDirVisuals AveragePercentage
