//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Extra Credit Assignment PageRank
// Due: December 2, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop.pagerank;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.junit.Test;

import edu.csueb.vhendrix.hadoop.pagerank.PageRankMapper;

public class PageRankMapperTest
{

    @SuppressWarnings(
    { "unchecked" })
    @Test
    public void processesValidRecord() throws IOException, InterruptedException
    {
        PageRankMapper mapper = new PageRankMapper();

        Text key = new Text("wwww.how.com");
        Text value = new Text("wwww.how.com\t0.1,{www.me.com www.you.com www.us.com}");
        @SuppressWarnings("rawtypes")
        Context context = mock(Context.class);

        mapper.map(key, value, context);

        // Verify certain behavior happened at least once and also
        // verify that it had 3 times

        verify(context, times(1)).write(new Text("www.me.com"),
                new Text("0.03333333333333333"));
        verify(context, times(1)).write(new Text("www.you.com"),
                new Text("0.03333333333333333"));
        verify(context, times(1)).write(new Text("www.us.com"),
                new Text("0.03333333333333333"));
        verify(context, times(1)).write(key,
                new Text("{www.me.com www.you.com www.us.com}"));
    }

   
}
