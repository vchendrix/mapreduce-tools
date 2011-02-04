//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop.wordcount;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;
import edu.csueb.vhendrix.hadoop.wordcount.WordCountMapper;
import edu.csueb.vhendrix.hadoop.wordcount.WordCountMapper.Counters;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

public class WordCountMapperTest
{

	@SuppressWarnings({ "unchecked" })
	@Test
	public void processesValidRecord() throws IOException, InterruptedException
	{
		WordCountMapper mapper = new WordCountMapper();

		Text value = new Text(
		        "We the People of the United States,\nin Order to form a more perfect\n"
		                + "Union, establish Justice,\ninsure domestic Tranquility, provide for the\n"
		                + "common defence,\npromote the general Welfare,\nand secure the Blessings of\n"
		                + "Liberty to ourselves and our Posterity,\ndo ordain and establish this\n"
		                + "Constitution for the United States of America.");
		@SuppressWarnings("rawtypes")
		Context context = mock(Context.class);
		Counter mockCounter = mock(Counter.class);
		when(context.getCounter(Counters.INPUT_WORD)).thenReturn(mockCounter);
		 
		mapper.map(new Object(), value, context);

		// Verify certain behavior happened atleast once and alseo
		// verify that it had 33 times
		verify(context, times(33)).write(new Text("america"), new IntWritable(1));
	}
	
	@SuppressWarnings({ "unchecked" })
    @Test
    public void processesValidCooccurRecord() throws IOException, InterruptedException
    {
        WordCountMapper mapper = new WordCountMapper();

        Text value = new Text(
                "We the People of the United States,\nin Order to form a more perfect\n"
                        + "Union, establish Justice,\ninsure domestic Tranquility, provide for the\n"
                        + "common defence,\npromote the general Welfare,\nand secure the Blessings of\n"
                        + "Liberty to ourselves and our Posterity,\ndo ordain and establish this\n"
                        + "Constitution for the United States of America.");
        @SuppressWarnings("rawtypes")
        Context context = mock(Context.class);
        Counter mockCounter = mock(Counter.class);
        Configuration job = mock(Configuration.class);
        when(context.getCounter(Counters.INPUT_WORD)).thenReturn(mockCounter);
        when(context.getCounter(Counters.INPUT_WORD)).thenReturn(mockCounter);
        when(job.getBoolean("wordcount.cooccur", false)).thenReturn(Boolean.TRUE);    
        mapper.configure(job);
        mapper.map(new Object(), value, context);

        // Verify certain behavior happened atleast once and alseo
        // verify that it had 33 times
        verify(context, times(33)).write(new Text("america -- states"), new IntWritable(1));
    }
}
