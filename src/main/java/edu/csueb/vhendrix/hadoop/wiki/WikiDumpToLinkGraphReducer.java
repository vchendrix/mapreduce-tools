package edu.csueb.vhendrix.hadoop.wiki;
//###########################################################################
//Val Hendrix
//CS6580 - Distributed systems
//Extra Credit Assignment PageRank
//Due: December 2, 2010
//##########################################################################


import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WikiDumpToLinkGraphReducer extends Reducer<Text, Text, NullWritable, Text>
{
    
    private static final Text LINE = new Text();
    private static StringBuffer value=new StringBuffer();

    protected void reduce(Text key, Iterable<Text> values,
            Context context)
            throws IOException, InterruptedException
    {
        Iterator<Text> itr = values.iterator();
        value.append(key.toString());
        value.append(',');
        while (itr.hasNext())
        {
            value.append(itr.next().toString());
            if(itr.hasNext()) value.append(',');
            
        }
        LINE.set(value.toString());
        value.delete(0, value.length());
        context.write(NullWritable.get(),LINE);
    }

}
