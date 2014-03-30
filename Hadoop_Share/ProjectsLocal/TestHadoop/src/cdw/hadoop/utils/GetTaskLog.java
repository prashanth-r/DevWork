package cdw.hadoop.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapred.DefaultJobHistoryParser;
import org.apache.hadoop.mapred.JobHistory;
import org.apache.tika.exception.TikaException;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.html.HtmlParser;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

public class GetTaskLog {

	/**
	 * This is to save the task logs, which are local to where the task executes.
	 * These logs are essential, especially to capture details regarding errors (Exception, stack trace and other info).
	 * The TaskTracker deletes the log files when they "old".  So they need to be retrieved and saved.
	 * 
	 * There is no direct API mechanism to retrieve the logs.
	 * This program makes use of an "older" API. However this appears to be only mechanism with the hadoop version in CDH4.
	 * The History server REST API when setup, may be the one to use to get the tasklog URL.
	 * However, once the URL is obtained, it is still by "scraping" that the logs can be saved.
	 * Other log "consolidation", that apparently CDH5 offers, can be looked into.
	 * Perhaps there are some more options, ex: "HDFS file appender" for log4j. etc.
	 * 
	 * @param args
	 * 
	 */
	
	private static final Log LOG = LogFactory.getLog(GetTaskLog.class);
	
	private static final String HISTORY_LOG_DIR_SUFFIX = "_logs/history";
	
	private static PathFilter jobLogFilter = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return !(path.getName().endsWith("xml"));
		}
	};
	
	public static void main(String[] args) throws IOException {
		

		/*
		 * Starting with the output directory supplied, first get the history log file.
		 * This should be in <job output dir>/_logs/history and a single file that is not with .xml extension.
		 */
		
		if (args.length != 2) {
			System.err.println("Invalid arguments. Usage "+GetTaskLog.class.getName()+" <HDFS Job log directory> <HDFS Task log directory>");
		}
		String jobOutputDirName = args[0];	/* The output directory name specified on the mapreduce job */
		String taskLogsDirName = args[1];	/* The directory to place the logs of all the corresponding tasks */
		Path jobOutputDirPath = new Path(jobOutputDirName);
		Path historyLogDirPath = new Path(jobOutputDirPath, HISTORY_LOG_DIR_SUFFIX);
		Configuration conf = new Configuration();	/* Default or Site */

		FileSystem fs;
//		fs = historyLogDirPath.getFileSystem(conf);
		fs = FileSystem.get(conf);

		if (!fs.exists(jobOutputDirPath) || !fs.getFileStatus(jobOutputDirPath).isDirectory()) {
			throw new IOException("Job output directory " + jobOutputDirPath.toString()+ " does not exist or is not a directory.");
		}
		
		if (!fs.exists(historyLogDirPath) || !fs.getFileStatus(historyLogDirPath).isDirectory()) {
			throw new IOException("Job history log directory " + historyLogDirPath.toString()+ " does not exist or is not a directory.");
		}
		
		Path[] jobFiles = FileUtil.stat2Paths(fs.listStatus(historyLogDirPath, jobLogFilter));
		if (jobFiles.length == 0) {
			throw new IOException("Not a valid History Directory: "+historyLogDirPath.toString());
		}
		LOG.debug("getName()="+jobFiles[0].getName()+",  toString()="+jobFiles[0].toString());
		

		/*
		 * Parse the history log file
		 */
		
		/* This builds full object model.  Line by line is possible.  See JobHistory API */
		JobHistory.JobInfo job = new JobHistory.JobInfo(null);
		DefaultJobHistoryParser.parseJobTasks(jobFiles[0].toString(), job, fs);

		/*
		 * Set up the log file name on HDFS to output all of the tasks logs
		 */
		String jobName = job.get(JobHistory.Keys.JOBNAME);
		String jobId = job.get(JobHistory.Keys.JOBID);
		String taskLogsFileName = jobName + "_" + jobId + "_allTasks.log";
		Path taskLogsPath = new Path(taskLogsDirName, taskLogsFileName);
		
		/*
		 * Iterate through all the tasks and get the task log URL
		 */

		FSDataOutputStream outStream = fs.create(taskLogsPath);		/* default, overwrite if exists */
		PrintWriter wtr = new PrintWriter(outStream);

		Map<String, JobHistory.Task> allTasks = job.getAllTasks();
		for (JobHistory.Task task : allTasks.values()) {
			String taskType = task.get(JobHistory.Keys.TASK_TYPE);
			Map<String, JobHistory.TaskAttempt> attempts = task.getTaskAttempts();
			for (JobHistory.TaskAttempt attempt : attempts.values()) {
				LOG.debug("In all task attempts loop");
				String taskLogURL = JobHistory.getTaskLogsUrl(attempt);
				/* scheme of task log URL is expected to be http only */
				if (taskLogURL != null && taskLogURL.startsWith("http")) {
					String attemptId = attempt.get(JobHistory.Keys.TASK_ATTEMPT_ID);
					String logData = getLogData(taskType, attemptId, taskLogURL);
//					outStream.writeUTF(String.format("Task Type = %s,  Attempt Id = %s,  URL = %s%n", taskType, attemptId, taskLogURL));
//					outStream.writeUTF(String.format("%s%n", logData));
					wtr.printf("Task Type = %s,  Attempt Id = %s,  URL = %s%n", taskType, attemptId, taskLogURL);
					wtr.printf("%s%n", logData);
				}
			}
		}
		
//		outStream.close();
		wtr.close();
		fs.close();
		
	}
	
	
	private static String getLogData(String taskType, String attemptId, String taskLogURL) {
		LOG.debug("TaskType="+taskType+", attemptId="+attemptId+", URL="+taskLogURL);

		/* With CDH4, under hadoop, the Apache http client libraries are still old - from the commons.  So, using java.net.  */
		String logText = null;
		String fmtLogText = null;
		BufferedReader rdr = null;
		HttpURLConnection httpCon = null;
		InputStream is = null;
		
		try {
			URL url = new URL(taskLogURL);
			httpCon = (HttpURLConnection) url.openConnection();
			int respCode = httpCon.getResponseCode();
			if (respCode == HttpURLConnection.HTTP_OK) {
				is = httpCon.getInputStream();
			} else {
				LOG.error("Bad Status code: "+respCode);
				is = httpCon.getErrorStream();
			}

//			rdr = new BufferedReader(new InputStreamReader(is));
//			String line;
//			while ((line = rdr.readLine()) != null) {
//				log.debug(line);
//			}

			ContentHandler handler = new BodyContentHandler();
			Metadata meta = new Metadata();
			new HtmlParser().parse(is, handler, meta, new ParseContext());
			logText = handler.toString();
			if (logText != null) {
//				logText = logText.replaceAll("[\n\\s+\n]*", "");
				BufferedReader bufRdr = new BufferedReader (new StringReader(logText));
				StringBuffer strBuf = new StringBuffer();
				String aLine;
				while ((aLine = bufRdr.readLine()) != null) {
					if (aLine.trim().length() > 0) {
						strBuf.append(String.format("%s%n", aLine));
					}
				}
				bufRdr.close();
				fmtLogText = strBuf.toString();
			}
			if (is != null)
				is.close();

			LOG.debug(fmtLogText);
			
		} catch (MalformedURLException e) {
			LOG.error("", e);
		} catch (IOException e) {
			LOG.error("", e);
		} catch (SAXException e) {
			LOG.error("", e);
		} catch (TikaException e) {
			LOG.error("", e);
		} finally {
			if (httpCon != null)
				httpCon.disconnect();
			if (rdr != null)
				try {
					rdr.close();
				} catch (IOException e) {
					LOG.error("", e);
				}
		}
		return fmtLogText == null? "Error retrieving task logs!" : fmtLogText;
	}

}
