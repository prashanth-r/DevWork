package cdw.hadoop.utils;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

public class FieldCountCheck extends Configured implements Tool {
	
	static class FieldCountMap extends Mapper<LongWritable, Text, Text, Text> {
		
		private String fieldSeparator = null;
		private int fieldCount = 0;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			// TODO Can these actually be set up in the outer class? At the job level?
			Configuration conf = context.getConfiguration();
			fieldCount = conf.getInt("cdw.extfile.fieldCount", 0);
			fieldSeparator = conf.get("cdw.extfile.fieldSeparator");
			
			if (fieldCount == 0) {
				throw new IOException("Field count property \"cdw.extfile.fieldCount\" is invalid or not set!");
			}
			
			if (fieldSeparator == null) {
				throw new IOException("Field separator property \"cdw.extfile.fieldSeparator\" is invalid or not set!");
			}
		}
		
		
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String record = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(record, fieldSeparator);
			int tokenCount = tokenizer.countTokens();
			if (fieldCount != tokenCount) {
				throw new IOException("Field Count Check failed.  Expected="+fieldCount+", Actual="+tokenCount);
			}
		}
	}

	@Override
	public int run(String[] arg0) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
