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

/**
 * This {@link Reducer} sums up the new page rank
 * 
 * @author Val Hendrix 
 * 
 */
public class PageRankReducer extends
        Reducer<Text, Text, Text, Text>
{
    public static final double D=0.85;  // damping
    private double pr=0;
    private Text rankList= new Text();

    /**
     * 
     * @param key
     *            - the text key to reduce
     * @param values
     *            - An {@link Text} list of values to rank
     * @param context
     *            - this is the MapReduce's job context. You can find the
     *            configuration in here.
     */
    public void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException
    {
        String list="";
        Iterator<Text> itr = values.iterator();
        pr=0;
        while (itr.hasNext())
        {
            String val = itr.next().toString(); 
            if(!val.startsWith("{") && val.length()>0)
            // it is the page rank
            {
                try{
                pr+=Double.parseDouble(val);} catch(NumberFormatException nfe){nfe.printStackTrace();}
            }
            else list=val; // there should only be one list of urls
        }
        
        pr= (1-D)+(D*pr);
        rankList.set(Double.valueOf(pr)+","+list);
        context.write(key, rankList);
        

    }
}
