export OLH_HOME=/users/NUID/oraloader-2.3.1-h2

CUSTOM_JAR=/users/NUID/oracle_utils/customFormat.jar

export HADOOP_CLASSPATH=${OLH_HOME}/jlib/orai18n-mapping.jar:${OLH_HOME}/jlib/oraloader-examples.jar:${OLH_HOME}/jlib/jackson-mapper-asl-1.8.8.jar:${OLH_HOME}/jlib/jackson-core-asl-1.8.8.jar:${OLH_HOME}/jlib/osdt_cert.jar:${OLH_HOME}/jlib/commons-math-2.2.jar:${OLH_HOME}/jlib/orai18n-utility.jar:${OLH_HOME}/jlib/oraloader.jar:${OLH_HOME}/jlib/avro-1.7.3.jar:${OLH_HOME}/jlib/avro-mapred-1.7.3-hadoop2.jar:${OLH_HOME}/jlib/orai18n.jar:${OLH_HOME}/jlib/orai18n-collation.jar:${OLH_HOME}/jlib/oraclepki.jar:${OLH_HOME}/jlib/ojdbc6.jar:${OLH_HOME}/jlib/osdt_core.jar:${OLH_HOME}/jlib/ora-hadoop-common.jar:${CUSTOM_JAR}

## -D oracle.hadoop.loader.loaderMapFile=file:/users/NUID/oracle_utils/loaderMap_PR_SQOOP_EXP_TEST.xml \
## -D mapred.reduce.tasks=8 \
## -conf OCIOutput.xml

hadoop jar ${OLH_HOME}/jlib/oraloader.jar oracle.hadoop.loader.OraLoader -libjars ${CUSTOM_JAR} \
-D mapred.job.name=NWTAP_CLARITY_COL_01 \
-D mapred.reduce.tasks=0 \
-D mapred.map.tasks=10 \
-D mapred.input.dir=input/olh_test/CLARITY_COL_20140222_20140225_004318.EXT \
-D mapred.output.dir=/user/NUID/output/olh_test/NWTAP_CLARITY_COL_01 \
-D oracle.hadoop.loader.defaultDateFormat="MM/dd/yyyy HH:mm:ss" \
-D oracle.hadoop.loader.loaderMap.targetTable=NWTAP_CLARITY_COL \
-D cdw.hadoop.olh.stgload.batchId=$(date +%Y%m%d%H%M%S) \
-D cdw.hadoop.olh.stgload.dateFormats="MM/dd/yyyy HH:mm:ss,MM/dd/yyyy HH:mm,MM/dd/yyyy HH,MM/dd/yyyy" \
-conf /users/NUID/cdwstgload/config/oracle_connection_jdbc_thin.xml \
-conf input_conf.xml \
-conf /users/NUID/cdwstgload/config/map_CLARITY_COL.xml \
-conf jdbcOutput.xml
