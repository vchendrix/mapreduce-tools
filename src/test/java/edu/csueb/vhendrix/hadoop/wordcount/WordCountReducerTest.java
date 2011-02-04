//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop.wordcount;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Test;

import edu.csueb.vhendrix.hadoop.wordcount.WordCountReducer;

public class WordCountReducerTest {

	@SuppressWarnings("unchecked")
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		WordCountReducer reducer = new WordCountReducer();

		List<IntWritable> values = new ArrayList<IntWritable>();
		values.add(new IntWritable(4));
		values.add(new IntWritable(5));
		values.add(new IntWritable(6));
		values.add(new IntWritable(7));
		@SuppressWarnings("rawtypes")
		Context context = mock(Context.class);
		reducer.reduce(new Text("simone.rose@enron.com"),
				(Iterable<IntWritable>) values, context);

		verify(context).write(new Text("simone.rose@enron.com"),
				new IntWritable(22));
	}
}
