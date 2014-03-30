CUSTOM_JAR=/users/NUID/oracle_utils/getLogInfo.jar

export HADOOP_CLASSPATH=${CUSTOM_JAR}:/opt/cloudera/parcels/SOLR-1.1.0-1.cdh4.3.0.p0.21/lib/search/lib/tika-parsers-1.4.jar:/opt/cloudera/parcels/SOLR-1.1.0-1.cdh4.3.0.p0.21/lib/search/lib/tika-core-1.4.jar:/opt/cloudera/parcels/SOLR-1.1.0-1.cdh4.3.0.p0.21/lib/search/lib/tika-xmp-1.4.jar:/opt/cloudera/parcels/SOLR-1.1.0-1.cdh4.3.0.p0.21/lib/search/lib/tagsoup-1.2.1.jar:.

export HADOOP_CLIENT_OPTS="-Dlog4j.configuration=get_log4j.properties"
hadoop cdw.hadoop.olh.stgload.GetTaskLog \
fork/output/20140312135413/ZC_SENSE fork/output/20140312135413/ZC_SENSE/taskLogs
#fork/output/20140227234247/CLARITY_TBL_FK
#fork/output/20140227234247/CLARITY_TBL_FK/_logs/history/job_201402111024_0369_1393904640113_NUID_20140227234247_CLARITY_TBL_FK


#java -cp ${CUSTOM_JAR}:$(hadoop classpath) -Djob.history.filename=fork/output/20140227234247/CLARITY_TBL_FK/_logs/history/job_201402111024_0369_1393904640113_NUID_20140227234247_CLARITY_TBL_FK cdw.hadoop.olh.stgload.GetTaskLog
