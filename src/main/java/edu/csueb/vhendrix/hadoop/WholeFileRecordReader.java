//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * The {@link RecordReader} used by {@link WholeFileInputFormat} for reading a whole file as a
 * record modified from "Hadoop: The Definitive Guide, 1st Edition", pp 192-194. 
 * <p/>
 * This {@link InputFormat} takes a {@link FileFormat} and produces a single record from that file.
 * 
 * @author Val Hendrix
 * 
 */
class WholeFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

	private FileSplit fileSplit;
	private TaskAttemptContext context;
	private boolean processed = false;
	private BytesWritable value;
	FileSystem fs;

	@Override
	/**
	 * The progress will either be zero or one.  It will never
	 * be anything more than that because the entire file is treated as
	 * one record.
	 */
	public float getProgress() throws IOException {
		return processed?1.0f:0.0f;
	}

	@Override
	/**
	 * Since this treats an entire file as one record this method does nothing
	 */
	public void close() throws IOException {
		
	}

	@Override
	/**
	 * The current key is {@link NullWritable}
	 */
	public NullWritable getCurrentKey() throws IOException,
			InterruptedException {
		return NullWritable.get();
	}

	@Override
	/**
	 * After {@link WhileFileRecordReader#nextKeyValue} is called this 
	 * will have the entire contents of the file as a value. 
	 */
	public BytesWritable getCurrentValue() throws IOException,
			InterruptedException {
		return value;
	}

	@Override
	/**
	 * Initializes this object
	 */
	public void initialize(InputSplit split, TaskAttemptContext context)
			throws IOException, InterruptedException {

		if (split instanceof FileSplit) {
			this.fileSplit = (FileSplit) split;

		} else
			throw new IOException("InputSplit must be of type FileSplit");
		this.context = context;
	}

	@Override
	/**
	 * Reads the entire {@link FileSplit} and sets it as the current value.
	 */
	public boolean nextKeyValue() throws IOException, InterruptedException {
		if (!processed) {
			value = new BytesWritable();
			byte[] contents = new byte[(int) fileSplit.getLength()];
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(context.getConfiguration());
			FSDataInputStream in = null;
			try {
				in = fs.open(file);
				IOUtils.readFully(in, contents, 0, contents.length);
				value.set(contents, 0, contents.length);
			} finally {
				IOUtils.closeStream(in);
			}
			processed = true;
			return true;
		}
		return false;
	}

}
