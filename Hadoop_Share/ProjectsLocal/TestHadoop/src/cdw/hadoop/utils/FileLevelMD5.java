/**
 * 
 */
package cdw.hadoop.utils;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.MD5Hash;

/**
 * @author k228680
 *
 * Given a file on HDFS, compute the MD5 for it.
 * Streaming using md5sum command maybe another alternative.  Has to be tested if default or standard InputFormats will work.
 * 
 * Currently this is not a map reduce job, but can be turned into one by implementing a "Whole file" InputFormat
 * 
 */
public class FileLevelMD5 {

	/**
	 * @param args
	 */
	private static final Log LOG = LogFactory.getLog(FileLevelMD5.class);

	public static void main(String[] args) throws IOException {

		if (args.length != 1) {
			System.err.println("Invalid arguments. Usage "+FileLevelMD5.class.getName()+" <HDFS File Name>");
		}
		
		Path inpHDFSFilePath = new Path(args[0]);

		Configuration conf = new Configuration();	/* Default or Site */

		FileSystem fs = FileSystem.get(conf);

		if (!fs.exists(inpHDFSFilePath) || !fs.getFileStatus(inpHDFSFilePath).isFile()) {
			throw new IOException("Input file name " + inpHDFSFilePath.toString()+ " does not exist or is not a directory.");
		}
		
		FSDataInputStream inputStream = fs.open(inpHDFSFilePath);
		MD5Hash md5Hash = MD5Hash.digest(inputStream);
		String md5Value = md5Hash.toString();
		LOG.info(String.format("File = \"%s\",  MD5 value = \"%s\"%n",inpHDFSFilePath.toString(),md5Value));
	}

}
