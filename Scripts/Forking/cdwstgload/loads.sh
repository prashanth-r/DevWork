# Catch all errors
trap 'echo "${0}: line ${LINENO}: exit status of last command: ${?}" 1>&2;exit 1' ERR

BASE_HDFS_DIR=/user/k228680/fork
DATA_LOCAL_DIR=${2}
PGM_LOCAL_DIR=/users/k228680/cdwstgload
CONFIG_DIR=${PGM_LOCAL_DIR}/config
LOG_BASE_DIR=${PGM_LOCAL_DIR}/logs
TABLE_LIST_FILE=${1}
CHECK_SUM_FILE=${CONFIG_DIR}/check_sum.txt
DELIM="\t"

## Load up a key-value map of table and associated files
declare -A tabFileMap
declare -a files=$( find ${DATA_LOCAL_DIR} -type f -name "*.EXT" ! -empty | sort -u )
for fname in ${files[@]}
do
   baseName=$(basename ${fname})
   tabName=$(echo ${baseName} | sed "s/\(.*\)\(\(_[[:digit:]]\{6,8\}\)\{3\}.*\)/\1/" )
   ## the value will be a list of files, just in case there is more than one per table
   tabFileMap[${tabName}]+=" ${fname}"
done

## Old batch or new batch?
batchIdFile=${CONFIG_DIR}/batchId.txt
if [ -s ${batchIdFile} ]; then
   BATCH_ID=$(< ${batchIdFile})
else
   BATCH_ID=$(date +%Y%m%d%H%M%S)
   echo ${BATCH_ID} > ${batchIdFile}
fi

logsDir=${LOG_BASE_DIR}/${BATCH_ID}
mkdir -p ${logsDir}
loadStatFile=${logsDir}/loadStats.txt

## Create all HDFS directories
inputDirHDFS=${BASE_HDFS_DIR}/input
processDirHDFS=${BASE_HDFS_DIR}/process/${BATCH_ID}
archiveDirHDFS=${BASE_HDFS_DIR}/archive/${BATCH_ID}
outputDirHDFS=${BASE_HDFS_DIR}/output/${BATCH_ID}
hadoop fs -mkdir -p ${processDirHDFS} ${outputDirHDFS} ${archiveDirHDFS}

while read line
do
   tableName=${line/,*/}
   colCount=${line/*,/}
   echo "Processing ${tableName}"

   ## List of EXT files corresponding to the table
   #declare -a files=($(find ${DATA_LOCAL_DIR} -regextype sed -regex "${DATA_LOCAL_DIR}/${tableName}\(_[[:digit:]]\{8\}\)\{2\}_[[:digit:]]\{6\}\.EXT" ! -empty | sort))
   #if [ -z ${files} ]; then
   if [ -z ${tabFileMap[${tableName}]} ]; then
      echo "No files found for table ${tableName}"
      continue
   fi

   #for extFile in "${files[@]}"
   for extFile in "${tabFileMap[${tableName}][@]}"
   do

      ## Check for duplicate files
      declare -a checkSumInfo=($(md5sum ${extFile}))
      if [ $(grep -c "${checkSumInfo[0]}" "${CHECK_SUM_FILE}") -gt "0" ]; then
         echo "Duplicate file: " ${extFile} 1>&2
         continue
      fi

      ## Check field counts and get record count
      declare -a recInfo=( $(awk -F"${DELIM}" -v colCount=${colCount} '{if (NF != colCount){exit}} END{printf("%d %d",NF,NR)}' ${extFile}) )
      if [ ${recInfo[0]} != "${colCount}" ]; then
         echo "Excpected field count: ${colCount}, but found only: ${recInfo[0]}, in file ${extFile} at record number ${recInfo[1]}" 1>&2
         continue
      fi

      ## Run OLH
      baseEXTFile=$(basename ${extFile})
      olhOutputDirHDFS=${outputDirHDFS}/${tableName}
      olhJobName=${BATCH_ID}_${tableName}
      taskLogsDirHDFS=${olhOutputDirHDFS}/taskLogs

      hadoop fs -rm -f -skipTrash ${inputDirHDFS}/${baseEXTFile}
      hadoop fs -put ${extFile} ${inputDirHDFS}/${baseEXTFile}
      nohup ./olhLoad.sh \
             ${olhJobName} ${baseEXTFile} ${recInfo[1]} ${inputDirHDFS} ${processDirHDFS} ${archiveDirHDFS} ${olhOutputDirHDFS} \
             ${CONFIG_DIR} ${taskLogsDirHDFS} ${tableName} "NWTAP_" ${BATCH_ID} ${loadStatFile} \
             >> ${logsDir}/load_${tableName}.out 2>> ${logsDir}/load_${tableName}.err &

      echo "PID ${!} submitted for table ${tableName} and file ${baseEXTFile}"
      echo ${checkSumInfo[@]} >> ${CHECK_SUM_FILE}
   done
done < ${TABLE_LIST_FILE}

## Wait for all loads (child processes) to complete
wait

echo "All runs ended"

## Check if all loads were successful to decide on deleting the batch id file
if [ -s ${loadStatFile} ]; then
   ## very simple logic for now
   if [ $(grep -c "^Problem" ${loadStatFile}) == "0" ]; then
      rm ${batchIdFile}
   fi
fi
