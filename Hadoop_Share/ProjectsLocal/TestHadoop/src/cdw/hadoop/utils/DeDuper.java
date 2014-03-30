package cdw.hadoop.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.LazyOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class DeDuper extends Configured implements Tool {
	
	/*
	 * MultiInput.  
	 * Set B - Set A.
	 * 
	 * Set A.
	 *  - Contains only MD5 values
	 *  - KeyValueTextInputFormat
	 *  - Identity Mapper
	 *  
	 * Set B.
	 *  - Contains only input text. MD5 values to be computed.
	 *  - TextInputFormat
	 *  - Mapper to produce MD5 values as keys for the reducer
	 *  
	 * The Mapper works essentially like an Identity Mapper for Set A, while computing the MD5 values for Set B
	 * The key becomes the MD5Hash and the values list has 3 possibilities 
	 *  - "empty" from Set A (one value per key) 
	 *  - the text from Set B (one value per key)
	 *  - combination of the above two (two values per key)
	 *  
	 *  A Combiner is NOT possible with this setup as key grouping is NOT guaranteed for a Combiner!!
	 * 
	 */
	
	private static final Log LOG = LogFactory.getLog(DeDuper.class);
	
	public static class DDMapper extends Mapper<Object, Text, MD5Hash, Text> {
		
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			MD5Hash md5Val = null;
			
			// InputSplit with md5 or values on which to compute md5
			String valueStr = value.toString();
			if (valueStr.isEmpty()) {
				String keyStr = ((Text) key).toString();
				md5Val = new MD5Hash(keyStr);
				LOG.info("Key="+keyStr+", MD5 conversion="+md5Val.toString());
			} else {
				md5Val = MD5Hash.digest(valueStr);
			}
			
			// TODO: check passing of Text object as is
			context.write(md5Val, value);
		}
	}
	
	
	public static class DDReducer extends Reducer<MD5Hash, Text, NullWritable, Text> {
		
		private MultipleOutputs<NullWritable, Text> multipleOutputs;
		
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			multipleOutputs = new MultipleOutputs<NullWritable, Text>(context);
		}
		
		@Override
		public void reduce(MD5Hash key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// The values Iterable should have a max of 2 occurrences
			// The only one that matters is the one where there in only one value and that value is non-empty
			// Iterable does not have a count().  So have to count in the program.
			
			int valueCount = 0;				// How many values for this key
			int nonEmptyValueCount = 0;		// How many non empty values for this key
			Text outputValue = null;
			
			for (Text value: values) {
				valueCount++;
				String valueStr = value.toString();
				if (!valueStr.isEmpty()) {
					// There should be only one non-empty value per key 
					if ((++nonEmptyValueCount) > 1) {
						// Shouldn't happen
						LOG.error("Multiple Records for key=\""+key.toString()+"\",  Value=\""+valueStr+"\"");
						throw new IOException("Multiple Records for key=\""+key.toString()+"\"");
					}
					outputValue = value;
					// Save the MD5s
					multipleOutputs.write(NullWritable.get(), new Text(key.toString()), "md5s/md5hash");
				}
			}

			// If keys were the same between Set A and Set B, valueCount is 2, otherwise 1.
			if (valueCount == 1 && outputValue != null) {
				// context.write(NullWritable.get(), outputValue);
				multipleOutputs.write(NullWritable.get(), outputValue, "diffs/diffVal");
			}
			
		}
		
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			multipleOutputs.close();
		}
	}
	
	
	@Override
	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s [generic options] <input file 1> <input file 2> <output>%n", getClass().getSimpleName());
			ToolRunner.printGenericCommandUsage(System.err);
			return -1;
		}
		
		Job job = new Job(getConf(), "DeDupJob");

		MultipleInputs.addInputPath(job, new Path(args[0]), KeyValueTextInputFormat.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setMapperClass(DDMapper.class);
		job.setReducerClass(DDReducer.class);
		job.setMapOutputKeyClass(MD5Hash.class);
		job.setMapOutputValueClass(Text.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		LazyOutputFormat.setOutputFormatClass(job, TextOutputFormat.class);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new DeDuper(), args);
		System.exit(exitCode);
	}

}
