package cdw.hadoop.utils;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

public class CheckListStatus {

	/**
	 * @param args
	 */
	private static PathFilter jobLogFilter = new PathFilter() {
		@Override
		public boolean accept(Path path) {
			return !(path.getName().endsWith("xml"));
		}
	};

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration();
		Path path = new Path(args[0]);
		// FileSystem fs = FileSystem.get(conf);
		FileSystem fs = path.getFileSystem(conf);
		// fs.getFileStatus(path);
		fs.listStatus(path, jobLogFilter);

	}

}
