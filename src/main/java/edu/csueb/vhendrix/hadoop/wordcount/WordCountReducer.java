//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * This {@link Reducer} counts the number of occurrences for each key.
 * 
 * @author Val Hendrix 
 * 
 */
public class WordCountReducer extends
        Reducer<Text, IntWritable, Text, IntWritable>
{
    private IntWritable result = new IntWritable();

    /**
     * 
     * @param key
     *            - the text key to reduce
     * @param values
     *            - An {@link IntWritable} list of values to sum
     * @param context
     *            - this is the MapReduce's job context. You can find the
     *            configuration in here.
     */
    public void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException
    {
        int sum = 0;
        for (IntWritable val : values)
        {
            sum += val.get();
            if (sum % 100 == 0)
                context.setStatus(String.format("Processing: Sum of %s is %s",
                        key, sum));
        }
        result.set(sum);
        context.setStatus(String.format("Finished: Sum of %s is %s", key, sum));
        context.write(key, result);

    }
}
