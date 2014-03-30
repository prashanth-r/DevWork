package cdw.hadoop.olh.stgload;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;
import java.util.UUID;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

public class StagingCustomRecordReader extends RecordReader<Object, IndexedRecord> {

	/**
	 * @param args
	 */

	private static final Log LOG = LogFactory.getLog(StagingCustomRecordReader.class);
	/* See OLH documentation */
	private RecordReader<Object, IndexedRecord> origRecord;

	/*
	 * List of additional data elements to be produced for the staging load
	 * At the time of this writing, these elements are not required to be persisted on the HDFS files, only in the CDW staging tables.
	 * So the generation of the same, only at run time.
	 * 
	 * TODO: It appears that this object is created once, but getCurrentValue() is called multiple times?!
	 */

	private String loadBatchId = null;
//	private String etlInsertDateString = null;
//	private String etlInsertDatePattern = null;
	private String forkExtractDateString = null;
//	private String forkExtractDatePattern = null;
	private String forkDataDateString = null;
//	private String forkDataDatePattern = null;
	private List<String> datePatterns = null;
	private List<String> dateFields = null;
	private String defaultDatePattern = null;
	
	
	public StagingCustomRecordReader(RecordReader<Object, IndexedRecord> rec, Configuration jobConf) throws IOException {

		origRecord = rec;
		LOG.debug("Constructor()");
		/* Retrieve "common" properties once */
		getPropertiesFromConfig(jobConf);
		
	}

	@Override
	public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		LOG.debug("initialize()");
		origRecord.initialize(split, context);
		
	}


	@Override
	public Object getCurrentKey() throws IOException, InterruptedException {
		Object o = origRecord.getCurrentKey();
		LOG.debug("getCurrentKey(), Key="+o);
		return o;
	}


	@Override
	public IndexedRecord getCurrentValue() throws IOException, InterruptedException {

		IndexedRecord idxRec = origRecord.getCurrentValue();
		
		/* 
		 * Using Reflection it was found that the concrete class is actually GenericRecord.
		 * So, using GenericRecord's methods to manipulate data "on the fly"
		 */
		
		if (!(idxRec instanceof GenericRecord)) {
			/* TODO: This should happen only if the OLH code changes. So check during upgrades! */
			throw new IOException("IndexedRecord class " + ReflectionUtils.getClass(idxRec).getName() + 
					" is not of type GenericRecord!!");
		}
		GenericRecord genericRec = (GenericRecord) idxRec;

		LOG.debug("Data BEFORE mod: " + idxRec);

		if (dateFields != null) {
			reformatDateValues(genericRec);
		}

		addCommonElementValues(genericRec);

		LOG.debug("Data AFTER mod: " + idxRec);

		return idxRec;
	}

	@Override
	public float getProgress() throws IOException, InterruptedException {
		return origRecord.getProgress();
	}

	@Override
	public boolean nextKeyValue() throws IOException, InterruptedException {
		return origRecord.nextKeyValue();
	}

	@Override
	public void close() throws IOException {
		origRecord.close();
		
	}


	private void getPropertiesFromConfig(Configuration jobConf) throws IOException {

		defaultDatePattern = jobConf.get("oracle.hadoop.loader.defaultDateFormat");
		datePatterns = convertCSVStringToList(jobConf.get("cdw.hadoop.olh.stgload.dateFormats"));
		
		/* If a table does not have date fields, the property is not expected */
		String dateFieldsProp = jobConf.get("cdw.hadoop.olh.stgload.dateFields");
		if (dateFieldsProp != null && dateFieldsProp.trim().length() > 0) {
			dateFields = convertCSVStringToList(dateFieldsProp);
		}
		loadBatchId = jobConf.get("cdw.hadoop.olh.stgload.batchId");
		
		/* 
		 * Parse the input file name to get Extract date and Data date.
		 * Format of file name is <Table Name>_<Data Date in YYYYMMDD>_<Extract date in YYYYMMDD_HHMMSS>.EXT
		 */
		
		/* TODO: Verify this way of getting the file name.  Per mapreduce tutorial and others, the property name is different, but returns null here! */
		String inputFileNameWithExtn = jobConf.get("mapred.input.dir");
		if (inputFileNameWithExtn == null) {
			throw new IOException("File name from property mapred.input.dir is NULL!");
		}
		
		String fileNamePattern = "^.*_\\d{8}_\\d{8}_\\d{6}\\.EXT$";
		if (!inputFileNameWithExtn.matches(fileNamePattern)) {
			throw new IOException("File name \"" + inputFileNameWithExtn + "\" does not match \"" + fileNamePattern +"\"");
		}
		String inputFileName = inputFileNameWithExtn.replaceAll("\\.EXT$", "");
		String[] fileNameParts = inputFileName.split("_");
		int fileNamePartsCount = fileNameParts.length;
		String extractDateStr = fileNameParts[fileNamePartsCount - 2] + " " + fileNameParts[fileNamePartsCount - 1];
		String dataDateStr = fileNameParts[fileNamePartsCount - 3];
		try {
			forkExtractDateString = convertDateFormat("yyyyMMdd HHmmss", defaultDatePattern, extractDateStr);
			forkDataDateString = convertDateFormat("yyyyMMdd", defaultDatePattern, dataDateStr);
		} catch (ParseException e) {
			throw new IOException(e);
		}
		
//		etlInsertDateString = jobConf.get("cdw.hadoop.olh.stgload.etlInsertDate");
//		etlInsertDatePattern = jobConf.get("cdw.hadoop.olh.stgload.etlInsertDateFormat");
		
//		forkExtractDateString = jobConf.get("cdw.hadoop.olh.stgload.forkExtractDate");
//		forkExtractDatePattern = jobConf.get("cdw.hadoop.olh.stgload.forkExtractDateFormat");

//		forkDataDateString = jobConf.get("cdw.hadoop.olh.stgload.forkDataDate");
//		forkDataDatePattern = jobConf.get("cdw.hadoop.olh.stgload.forkDataDateFormat");

	}
	
	
	private List<String> convertCSVStringToList(String csvString) {
		StringTokenizer tokenizer = new StringTokenizer(csvString, ",");
		ArrayList<String> tokens = new ArrayList<String>();
		while (tokenizer.hasMoreTokens()) {
			tokens.add(tokenizer.nextToken());
		}
		return tokens;
	}
	
	
	private void addCommonElementValues(GenericRecord genericRec) {
		UUID uuid = UUID.randomUUID();
		ByteBuffer byteBuf = ByteBuffer.wrap(new byte[16]);
		byteBuf.putLong(uuid.getMostSignificantBits()).putLong(uuid.getLeastSignificantBits());
		byte[] uuidBytes = byteBuf.array();
		genericRec.put("ETL_UNIQUE_KEY", uuidBytes);
		genericRec.put("ETL_PRCS_CD", "N");
		genericRec.put("ETL_INSRT_USER_ID", "OLH");
		genericRec.put("ETL_SRC_SYS_CD", "TAPESTRY");
		genericRec.put("ETL_BATCH_ID", loadBatchId);
		genericRec.put("ETL_INSRT_DTTM", (new SimpleDateFormat(defaultDatePattern)).format(new Date()));
		genericRec.put("FORK_EXTRACT_DTTM", forkExtractDateString);
		genericRec.put("FORK_DATA_DATE", forkDataDateString);
	}
	
	
	private void reformatDateValues(GenericRecord genericRec) {
		
		/*
		 * Date values in the Clarity EXT files are actually date+time values.
		 * However, the time part does not conform to a specific format.  SimpleDateFormat also does not allow for "optional" specifications.
		 * So, the date value is run through a few formats (should be ordered from most specific to the least specific) and then 
		 * formatted as per the "default" format so it can pass through the OLH load.
		 * TODO: Currently ETL_DATE is included in this list (thru config).  See if that can be separated out to be done once, probably in the InputFormat class.
		 */
		for (String dateField: dateFields) {
			Object o = genericRec.get(dateField);
			if ((o != null) && (o instanceof String)) {
				String dateValueStr = (String)o;
				if (dateValueStr.trim().length() > 0) {
					String formattedDateStr = null;
					for (String datePattern: datePatterns) {
						try {
							formattedDateStr = convertDateFormat(datePattern, defaultDatePattern, dateValueStr);
							/* Passed the parse without any Exception! So, exit loop */
							break;
						} catch (ParseException e) {
							/* If the format has an Exception, it should be thrown later anyway, by OLH */
						}
					}
					/* Do we have a (re)formatted Date String? */
					if (formattedDateStr != null) {
						/* Reset the value in the Record */
						genericRec.put(dateField, formattedDateStr);
					}
				}
			}
		}
	}
	
	
	private String convertDateFormat(String fromPatter, String toPattern, String dateString) throws ParseException {
		String retDateString = null;
		
		SimpleDateFormat fmt = new SimpleDateFormat();
		fmt.applyPattern(fromPatter);
		Date date = fmt.parse(dateString);
		fmt.applyPattern(toPattern);
		retDateString = fmt.format(date);
		return retDateString;
	}

//	public static void main(String[] args) {
//
//	}
}
