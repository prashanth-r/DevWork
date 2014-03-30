#trap 'echo "${0}: line ${LINENO}: exit status of last command: ${?}" 1>&2;exit 1' ERR

jobName=${1}
srcFile=${2}
recCount=${3}
inputDir=${4}
processDir=${5}
archiveDir=${6}
outputDir=${7}
configDir=${8}
taskLogsDir=${9}
targetTable=${10}
tablePrefix=${11}
batchId=${12}
loadStatFile=${13}

# Run the OLH load
export OLH_HOME=/users/NUID/oraloader-2.3.1-h2

CUSTOM_JAR=/users/NUID/oracle_utils/customFormat.jar

export HADOOP_CLASSPATH=${OLH_HOME}/jlib/orai18n-mapping.jar:${OLH_HOME}/jlib/oraloader-examples.jar:${OLH_HOME}/jlib/jackson-mapper-asl-1.8.8.jar:${OLH_HOME}/jlib/jackson-core-asl-1.8.8.jar:${OLH_HOME}/jlib/osdt_cert.jar:${OLH_HOME}/jlib/commons-math-2.2.jar:${OLH_HOME}/jlib/orai18n-utility.jar:${OLH_HOME}/jlib/oraloader.jar:${OLH_HOME}/jlib/avro-1.7.3.jar:${OLH_HOME}/jlib/avro-mapred-1.7.3-hadoop2.jar:${OLH_HOME}/jlib/orai18n.jar:${OLH_HOME}/jlib/orai18n-collation.jar:${OLH_HOME}/jlib/oraclepki.jar:${OLH_HOME}/jlib/ojdbc6.jar:${OLH_HOME}/jlib/osdt_core.jar:${OLH_HOME}/jlib/ora-hadoop-common.jar:${CUSTOM_JAR}

# Does the file exist?
hadoop fs -test -e ${inputDir}/${srcFile}
if [ ${?} == "0" ]; then
   # Is the file zero bytes?
   hadoop fs -test -z ${inputDir}/${srcFile}
   if [ ${?} == "0" ]; then
      hadoop fs -rm -f ${archiveDir}/${srcFile}
      hadoop fs -mv ${processDir}/${srcFile} ${archiveDir}/${srcFile}
      return
   else
      # rm -f just in case, to prevent mv errors
      hadoop fs -rm -f ${processDir}/${srcFile}
      hadoop fs -mv ${inputDir}/${srcFile} ${processDir}/${srcFile}
   fi
else
   echo "Input file  ${inputDir}/${srcFile} not found!"
   return 1
fi

## -D mapred.reduce.tasks=8 \
## -conf OCIOutput.xml
hadoop jar ${OLH_HOME}/jlib/oraloader.jar oracle.hadoop.loader.OraLoader -libjars ${CUSTOM_JAR} \
-D mapred.job.name=${jobName} \
-D mapred.reduce.tasks=0 \
-D mapred.map.tasks=4 \
-D mapred.input.dir=${processDir}/${srcFile} \
-D mapred.output.dir=${outputDir} \
-D oracle.hadoop.loader.loadByPartition=false \
-D oracle.hadoop.loader.defaultDateFormat="MM/dd/yyyy HH:mm:ss" \
-D oracle.hadoop.loader.enableSorting=false \
-D oracle.hadoop.loader.loaderMap.targetTable=${tablePrefix}${targetTable} \
-D cdw.hadoop.olh.stgload.batchId=${batchId} \
-D cdw.hadoop.olh.stgload.dateFormats="MM/dd/yyyy HH:mm:ss,MM/dd/yyyy HH:mm,MM/dd/yyyy HH,MM/dd/yyyy" \
-conf ${configDir}/oracle_connection_jdbc_thin.xml \
-conf ${configDir}/input_conf.xml \
-conf ${configDir}/map_${targetTable}.xml \
-conf ${configDir}/jdbcOutput.xml

if [ ${?} == "0" ]; then
   # rm -f just in case, to prevent mv errors
   hadoop fs -rm -f ${archiveDir}/${srcFile}
   hadoop fs -mv ${processDir}/${srcFile} ${archiveDir}/${srcFile}
fi


## Capture Tasklogs

CUSTOM_JAR=/users/NUID/oracle_utils/getLogInfo.jar

export HADOOP_CLIENT_OPTS="-Dlog4j.configuration=get_logs_log4j.properties"

export HADOOP_CLASSPATH=${CUSTOM_JAR}:/opt/cloudera/parcels/SOLR-1.1.0-1.cdh4.3.0.p0.21/lib/search/lib/tika-parsers-1.4.jar:/opt/cloudera/parcels/SOLR-1.1.0-1.cdh4.3.0.p0.21/lib/search/lib/tika-core-1.4.jar:/opt/cloudera/parcels/SOLR-1.1.0-1.cdh4.3.0.p0.21/lib/search/lib/tika-xmp-1.4.jar:/opt/cloudera/parcels/SOLR-1.1.0-1.cdh4.3.0.p0.21/lib/search/lib/tagsoup-1.2.1.jar:.

## One out of place item is that task logs dir has to be created only after the output directory is created
hadoop fs -mkdir -p ${taskLogsDir}
hadoop cdw.hadoop.olh.stgload.GetTaskLog \
${outputDir} ${taskLogsDir}

## Capture load stats from Oraloader log file

olhLogFile=${outputDir}/_olh/oraloader-report.txt
declare -a loadStats=( $(hadoop fs -cat ${olhLogFile} | grep "^Total[[:space:]]") )
if [[ -z ${loadStats} || ( ${#loadStats[@]} != 9 ) ]]; then
   echo "Problem loading table ${tableName} from ${srcFile}.  Load stats: ${loadStats[@]}" 1>&2
   return
fi

recsProcessed=${loadStats[4]}
recsInserted=${loadStats[5]}
if [[ ( ${recCount} != ${recsProcessed} ) || ( ${recCount} != ${recsInserted} ) ]]; then
   echo "Problem loading table ${tableName} from ${srcFile}. Recs in file: ${recCount}. Processed: ${recsProcessed}. Inserted: ${recsInserted}" >> ${loadStatFile}
else
   echo "Table ${tableName} successfully loaded from ${srcFile}. Recs in file: ${recCount}. Processed: ${recsProcessed}. Inserted: ${recsInserted}" >> ${loadStatFile}
fi
