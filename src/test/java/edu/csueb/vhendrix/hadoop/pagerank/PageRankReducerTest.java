//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop.pagerank;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.junit.Test;

import edu.csueb.vhendrix.hadoop.pagerank.PageRankReducer;

public class PageRankReducerTest {

	@SuppressWarnings("unchecked")
	@Test
	public void processesValidRecord() throws IOException, InterruptedException {
		PageRankReducer reducer = new PageRankReducer();

		List<Text> values = new ArrayList<Text>();
		values.add(new Text("0.04444"));
	    values.add(new Text("0.033333335"));
        values.add(new Text("0.025"));
		values.add(new Text("{www.me.com www.you.com www.us.com}"));
		@SuppressWarnings("rawtypes")
		Context context = mock(Context.class);
		reducer.reduce(new Text("wwww.how.com"),
				(Iterable<Text>) values, context);

		verify(context).write(new Text("wwww.how.com"),
				new Text("0.23735733475,{www.me.com www.you.com www.us.com}"));
	}
}
