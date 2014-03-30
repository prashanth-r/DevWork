package cdw.hadoop.olh.stgload;

import java.io.IOException;
import java.util.List;

import oracle.hadoop.loader.lib.input.DelimitedTextInputFormat;

import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.ReflectionUtils;

public class StagingCustomInputFormat extends InputFormat<Object, IndexedRecord> {

	/**
	 * @param args
	 */


	private static final Log LOG = LogFactory.getLog(StagingCustomInputFormat.class);

	/*
	 * Cannot directly subclass oracle.hadoop.loader.lib.input.DelimitedTextInputFormat.
	 * So using a "wrapper" style to wrap an instance of it and effectively subclass it that way.
	 * 
	 */
//	private DelimitedTextInputFormat inst = null;

	
//	public StagingCustomInputFormat() {
//		inst = new DelimitedTextInputFormat();
//		
//	}
	
	@Override
	public List<InputSplit> getSplits(JobContext context) throws IOException, InterruptedException {
		LOG.debug("In getSplits()");
		/*
		 * ReflectionUtils.newInstance vs new () instantiation is used. The difference or advantage is not obvious from any documentation.
		 * But it appears there may be caching that may save too many object creation and gc.
		 * See source oracle.hadoop.loader.examples.CSVInputFormat.java 
		 */
		DelimitedTextInputFormat dif = ReflectionUtils.newInstance(DelimitedTextInputFormat.class, context.getConfiguration());
		return dif.getSplits(context);
	}

	
	@Override
	public RecordReader<Object, IndexedRecord> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
		/* TODO: Thread Safety check */
		LOG.debug("In createRecordReader()");
		Configuration jobConf = context.getConfiguration();
		DelimitedTextInputFormat dif = ReflectionUtils.newInstance(DelimitedTextInputFormat.class, jobConf);
		return new StagingCustomRecordReader(dif.createRecordReader(split, context), jobConf);
	}
	

//	public static void main(String[] args) {
//		// TODO Auto-generated method stub
//
//	}

}
