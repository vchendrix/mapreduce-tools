package edu.csueb.vhendrix.hadoop.wiki;
//###########################################################################
//Val Hendrix
//CS6580 - Distributed systems
//Extra Credit Assignment PageRank
//Due: December 2, 2010
//##########################################################################

import java.io.IOException;
import java.io.StringReader;

import net.htmlparser.jericho.Element;
import net.htmlparser.jericho.Source;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;

import edu.csueb.vhendrix.hadoop.XmlInputFormat;

public class WikiDumpToLinkGraph extends Configured implements Tool
{

    @Override
    public int run(String[] args) throws Exception
    {
        int result = 0;
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: PageRank <wiki-dump.xml> <outputdir>");
            System.exit(2);
        }
        conf.set("xmlinput.start", "<page>");
        conf.set("xmlinput.end","</page>");
        Path inputFile = new Path(otherArgs[0]);
        Path outputFile = new Path(otherArgs[1]);
        
        
        Job job = new Job(conf, "WikiDumpToLinkGraph " + inputFile + " "+ outputFile);
        job.setJarByClass(WikiDumpToLinkGraph.class);
        job.setInputFormatClass(XmlInputFormat.class);
        job.setMapperClass(WikiDumpMapper.class);
        job.setReducerClass(WikiDumpToLinkGraphReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inputFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }
    
    /**
     * Counts the number of lines in a file
     * 
     * @author val
     * 
     */
    public static class WikiDumpMapper extends
            Mapper<Object, Text, Text, Text>
    {
        static Text PAGE_TITLE=new Text();
        static Text LINK=new Text();
        static WikiLinkAnalyzer wlAnalyzer = new WikiLinkAnalyzer();

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            Source source = new Source(new StringReader(Bytes.toString(value
                    .getBytes())));
            Element titleElement = source.getFirstElement("title");
            PAGE_TITLE.set(titleElement.getContent().toString());
            Element textElement = source.getFirstElement("text");
            
            wlAnalyzer.initialize(textElement.getContent().toString());
            wlAnalyzer.parse();
            for(String link: wlAnalyzer.getLinks())
            {
                LINK.set(link);
                context.write(PAGE_TITLE, LINK);
                 
            }

           
        }

    }
    
    
    
    

}
