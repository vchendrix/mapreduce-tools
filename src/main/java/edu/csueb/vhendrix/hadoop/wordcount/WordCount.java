package edu.csueb.vhendrix.hadoop.wordcount;
//###########################################################################
//Val Hendrix
//CS6580 - Distributed systems
//Assignment WordCooccurrences
//Due: November 23, 2010
//##########################################################################

import edu.csueb.vhendrix.hadoop.wordcount.WordCountMapper;
import edu.csueb.vhendrix.hadoop.wordcount.WordCountReducer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is the WordCount tool for use in finding word counts
 * and word co-occurrence counts.
 * 
 * @author Val Hendrix
 * 
 */
public class WordCount extends Configured implements Tool
{

	/**
	 * Used to run from the command line
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception
	{
		// Let ToolRunner handle generic command-line options
		int res = ToolRunner.run(new Configuration(), (Tool) new WordCount(),
		        args);

		System.exit(res);

	}

	/**
	 * Runs the tool
	 */
	public int run(String[] args) throws Exception
	{
		Configuration conf = getConf();
		String[] otherArgs = new GenericOptionsParser(conf, args)
		        .getRemainingArgs();
		if (otherArgs.length != 2)
		{
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}
		Job job = new Job(conf, "word count "+otherArgs[0]+" "+otherArgs[1]);
		job.setJarByClass(WordCount.class);
		job.setMapperClass(WordCountMapper.class);
		job.setCombinerClass(WordCountReducer.class);
		job.setReducerClass(WordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));


		return job.waitForCompletion(true) ? 0 : 1;
	}

}
