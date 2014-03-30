CUSTOM_JAR=/users/NUID/oracle_utils/FileMD5.jar

export HADOOP_CLASSPATH=${CUSTOM_JAR}:$(pwd)
export HADOOP_CLIENT_OPTS="-Dlog4j.configuration=get_log4j.properties"
hadoop cdw.hadoop.utils.FileLevelMD5 \
fork/archive/20140227234247/PLAN_GRP_BEN_PLAN_20140222_20140225_004318.EXT

