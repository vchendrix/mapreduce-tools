package edu.csueb.vhendrix.hadoop.wordcount;
//###########################################################################
//Val Hendrix
//CS6580 - Distributed systems
//Assignment WordCooccurrences
//Due: November 23, 2010
//##########################################################################

import edu.csueb.vhendrix.AnalyzerUtils;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.standard.StandardAnalyzer;


/**
 * The {@link Mapper} tokenizes the inputfile. Modified from
 * http://hadoop.apache.org/common/docs/r0.20.2/mapred_tutorial.html
 * 
 * @author Val Hendrix
 * 
 */
public class WordCountMapper extends
		Mapper<Object, Text, Text, IntWritable> {

    static Log LOG=	LogFactory.getLog(WordCountMapper.class);
	// Enumeration representing counters you will
    // see in the job tracker
	static enum Counters {
		INPUT_WORD
	}

	private final static IntWritable one = new IntWritable(1);
	private Text word = new Text();
	private boolean cooccur=false;
	private Set<String> patternsToSkip = new HashSet<String>();
	private long numRecords = 0;
	private String inputFile;

	/**
	 * This sets up the context for the job
	 */
	@Override
	protected void setup(Context context) throws IOException,
			InterruptedException {
		super.setup(context);
		this.configure(context.getConfiguration());
	}

	/**
	 * Configure the job.
	 * 
	 * @param job
	 */
	public void configure(Configuration job) {
		cooccur=job.getBoolean("wordcount.cooccur", false);
		inputFile = job.get("map.input.file");

		LOG.info("wordcount.skip.patterns="+job.getBoolean("wordcount.skip.patterns", false));
		LOG.info("wordcount.cooccur="+job.getBoolean("wordcount.cooccur", false));
		
		if (job.getBoolean("wordcount.skip.patterns", false)) {
			Path[] patternsFiles = new Path[0];
			try {
				patternsFiles = DistributedCache.getLocalCacheFiles(job);
			} catch (IOException ioe) {
				System.err
						.println("Caught exception while getting cached files: "
								+ StringUtils.stringifyException(ioe));
			}
			for (Path patternsFile : patternsFiles) {
				parseSkipFile(patternsFile);
			}
		}
	}

	/**
	 * Tokenizes the given text and collects it.
	 * 
	 * 
	 * @param key 
	 * @param value the text to process
	 * @param context this is the jobs context
	 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString();

		for (String pattern : patternsToSkip) {
			line = line.replace(pattern, "");
		}

		Token[] tokens =AnalyzerUtils.tokensFromAnalysis(new StandardAnalyzer(), line);
		for (Token t: tokens) {
		    
		    if(cooccur)
			{
		        for(Token t2:tokens)
		              if(!t.termText().equals(t2.termText()))word.set(t.termText()+" -- "+t2.termText());
			}
		    else
		        word.set(t.termText());
			context.write(word, one);
			context.getCounter(Counters.INPUT_WORD).increment(1);
		}

		if ((++numRecords % 100) == 0) {
			context.setStatus("Finished processing " + numRecords + " records "
					+ "from the input file: " + inputFile);
		}
	}
	
	/**
	 * This parses the patterns file and creates a {@link Set}
	 * of patterns to skip when tokenizing
	 * 
	 * @param patternsFile
	 */
	private void parseSkipFile(Path patternsFile) {
		try {
			BufferedReader fis = new BufferedReader(new FileReader(
					patternsFile.toString()));
			String pattern = null;
			while ((pattern = fis.readLine()) != null) {
				System.out.println(pattern);
				patternsToSkip.add(pattern);
			}
		} catch (IOException ioe) {
			System.err
					.println("Caught exception while parsing the cached file '"
							+ patternsFile + "' : "
							+ StringUtils.stringifyException(ioe));
		}
	}
}
