package edu.csueb.vhendrix.hadoop.pagerank;
//###########################################################################
//Val Hendrix
//CS6580 - Distributed systems
//Extra Credit Assignment PageRank
//Due: December 2, 2010
//##########################################################################

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlSortReducer extends Reducer<Text, Text, Text, Text>
{

    protected void reduce(Text key, Iterable<Text> values,
            Context context)
            throws IOException, InterruptedException
    {
        Iterator<Text> itr = values.iterator();
        while (itr.hasNext())
        {
            context.write(key, itr.next());
        }
    }

}
