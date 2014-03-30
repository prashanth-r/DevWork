CUSTOM_JAR=/users/NUID/oracle_utils/dedup_test.jar

export HADOOP_CLASSPATH=${CUSTOM_JAR}:$(pwd)

## -D oracle.hadoop.loader.loaderMapFile=file:/users/NUID/oracle_utils/loaderMap_PR_SQOOP_EXP_TEST.xml \
## -D mapred.reduce.tasks=8 \
## -conf OCIOutput.xml

##export HADOOP_CLIENT_OPTS="-Dlog4j.configuration=get_log4j.properties"

hadoop jar ${CUSTOM_JAR} cdw.hadoop.utils.DeDupJob -libjars ${CUSTOM_JAR} \
-D mapred.map.max.attempts=10 \
-D mapred.reduce.max.attempts=10 \
input/dedup_test/f00.txt \
fork/archive/20140227234247/PLAN_GRP_BEN_PLAN_20140222_20140225_004318.EXT \
output/dedup_test/out
