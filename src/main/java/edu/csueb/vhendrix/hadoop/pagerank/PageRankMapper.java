package edu.csueb.vhendrix.hadoop.pagerank;
//###########################################################################
//Val Hendrix
//CS6580 - Distributed systems
//Extra Credit Assignment PageRank
//Due: December 2, 2010
//##########################################################################

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


/**
 * The {@link Mapper} tokenizes the inputfile. Modified from
 * http://hadoop.apache.org/common/docs/r0.20.2/mapred_tutorial.html
 * 
 * @author Val Hendrix
 * 
 */
public class PageRankMapper extends
		Mapper<Object, Text, Text, Text> {

    static Log LOG=	LogFactory.getLog(PageRankMapper.class);
	// Enumeration representing counters you will
    // see in the job tracker
	static enum Counters {
		INPUT_WORD
	}

	private final static IntWritable one = new IntWritable(1);
	private double rank =0;
	private Text url = new Text();
	private Text urlList = new Text();
	private Text newRank= new Text();
	private String urlListStr="";
	private long numUrls = 0;

    List<String> urlsArray = new ArrayList<String>();

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
		
	}

	/**
	 * Tokenizes comma delimited file in the following format:
	 * url\trank, url1 url2 .... urln.
	 * 
	 * 
	 * @param key 
	 * @param value the text to process
	 * @param context this is the jobs context
	 */
	public void map(Object key, Text value, Context context)
			throws IOException, InterruptedException {

	    // Parse on tab to break the 
	    StringTokenizer st = new StringTokenizer(value.toString(),"\t");
	    url.set(st.nextToken()); // Get the key url
	    
	    // Parse on the comma to separate the rank from the url list
		st = new StringTokenizer(st.nextToken(),",");
		rank=Double.parseDouble(st.nextToken());

        urlListStr=(st.hasMoreTokens())?st.nextToken():"";
        
		// tokenize url list on space, count urls and 
		// create new link for the key url
	    st = new StringTokenizer(urlListStr," {}");
	    urlsArray.clear();
		while (st.hasMoreElements()) 
		{
		    String t=st.nextToken();
		    if(t!=null &&t.trim().length()>0)
		    {
		        numUrls++;
		        urlsArray.add(t);
		    }
		}
		
		// Create the new rank for the url and apply it to the 
		// urls in the list
		newRank.set(new Double(rank/numUrls).toString()); // floats end in f
        urlList.set(urlListStr);
		for(String u: urlsArray)
		    context.write(new Text(u),newRank);
		if(urlList.getLength()>0)
		    context.write(url, urlList);

		
	}
	
	
}
