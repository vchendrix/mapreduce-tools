//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.InvalidInputException;

/**
 * This is an {@link InputFormat} that treats the entire file as one record.
 * This has been modifed from "Hadoop Definitive Guide, 1st Edition", pp 192-194.
 * <p/>
 * This defines a format that does not use keys. Thus, {@link NullWritable} is
 * used as the input key and {@link BytesWritable} is used for the output value. 
 * This input format uses the {@link WholeFileRecordReader} that produces the
 * file contents as the record.
 * 
 * @author Val Hendrix
 * 
 */
public class WholeFileInputFormat extends
		FileInputFormat<NullWritable, BytesWritable> {
	private static final Log LOG = LogFactory
			.getLog(WholeFileInputFormat.class);

	private static final PathFilter hiddenFileFilter = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".");
		}
	};

	/**
	 * Proxy PathFilter that accepts a path only if all filters given in the
	 * constructor do. Used by the listPaths() to apply the built-in
	 * hiddenFileFilter together with a user provided one (if any).
	 */
	private static class MultiPathFilter implements PathFilter {
		private List<PathFilter> filters;

		public MultiPathFilter(List<PathFilter> filters) {
			this.filters = filters;
		}

		public boolean accept(Path path) {
			for (PathFilter filter : filters) {
				if (!filter.accept(path)) {
					return false;
				}
			}
			return true;
		}
	}

	@Override
	/**
	 * This file is not splitable
	 */
	protected boolean isSplitable(JobContext context, Path filename) {
		return false;
	}

	@Override
	/**
	 * Creates a {@link RecordReader} the {@link FileInput} must be a
	 * {@link FileSplit}
	 */
	public RecordReader<NullWritable, BytesWritable> createRecordReader(
			InputSplit split, TaskAttemptContext context) throws IOException,
			InterruptedException {
		RecordReader<NullWritable, BytesWritable> rr = new WholeFileRecordReader();
		rr.initialize(split, context);
		return rr;
	}

	@Override
	/**
	 * Overrides the superclass to add the ability to recursively descend into a directory
	 * tree to look for files. 
	 * <p/>
	 * Modifed from patch from jira:<br/>
	 * https://issues.apache.org/jira/browse/MAPREDUCE-1501?page=com.atlassian.jira.plugin.system.issuetabpanels:comment-tabpanel&focusedCommentId=12840122#action_12840122
	 */
	protected List<FileStatus> listStatus(JobContext job) throws IOException {
		List<FileStatus> result = new ArrayList<FileStatus>();
		List<IOException> errors = new ArrayList<IOException>();

		Path[] dirs = getInputPaths(job);
		if (dirs.length == 0) {
			throw new IOException("No input paths specified in job");
		}

		// creates a MultiPathFilter with the hiddenFileFilter and the
		// user provided one (if any).
		List<PathFilter> filters = new ArrayList<PathFilter>();
		filters.add(hiddenFileFilter);
		PathFilter jobFilter = getInputPathFilter(job);
		if (jobFilter != null) {
			filters.add(jobFilter);
		}
		PathFilter inputFilter = new MultiPathFilter(filters);

		for (int i = 0; i < dirs.length; ++i) {
			Path p = dirs[i];
			FileSystem fs = p.getFileSystem(job.getConfiguration());
			FileStatus[] matches = fs.globStatus(p, inputFilter);
			if (matches == null) {
				errors.add(new IOException("Input path does not exist: " + p));
			} else if (matches.length == 0) {
				errors.add(new IOException("Input Pattern " + p
						+ " matches 0 files"));
			} else {
				for (FileStatus globStat : matches) {
					if (globStat.isDir()) {
						for (FileStatus stat : fs.listStatus(
								globStat.getPath(), inputFilter)) {
							if (stat.isDir()) {
								addInputPathRecursively(result, fs,
										stat.getPath(), inputFilter);
							} else {
								result.add(stat);
							}
						}
					} else {
						result.add(globStat);

					}
				}
			}
		}

		if (!errors.isEmpty()) {
			throw new InvalidInputException(errors);
		}
		LOG.info("Total input paths to process : " + result.size());
		return result;
	}

	/**
	 * Add files in the input path recursively into the results.
	 * 
	 * @param result
	 *            The List to store all files.
	 * @param fs
	 *            The FileSystem.
	 * @param path
	 *            The input path.
	 * @param inputFilter
	 *            The input filter that can be used to filter files/dirs.
	 * @throws IOException
	 */
	protected void addInputPathRecursively(List<FileStatus> result,
			FileSystem fs, Path path, PathFilter inputFilter)
			throws IOException {
		for (FileStatus stat : fs.listStatus(path, inputFilter)) {
			if (stat.isDir()) {
				addInputPathRecursively(result, fs, stat.getPath(), inputFilter);
			} else {
				result.add(stat);
			}
		}
	}

}
