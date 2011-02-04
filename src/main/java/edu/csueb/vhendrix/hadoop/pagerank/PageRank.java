package edu.csueb.vhendrix.hadoop.pagerank;

//###########################################################################
//Val Hendrix
//CS6580 - Distributed systems
//Extra Credit Assignment PageRank
//Due: December 2, 2010
//##########################################################################

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.lib.KeyFieldBasedComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This is the PageRank tool for ranking web pages based on a link graph The
 * sparse matrix graph file should be in the following format: {urlKey},{url1,{url2},...{urln}
 * 
 * 
 * @author Val Hendrix
 * 
 */
public class PageRank extends Configured implements Tool
{

    /**
     * Used to run from the command line
     * 
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception
    {
        // Let ToolRunner handle generic command-line options
        int res = ToolRunner.run(new Configuration(), (Tool) new PageRank(),
                args);

        System.exit(res);

    }

    /**
     * Runs the tool
     */
    public int run(String[] args) throws Exception
    {
        int result = 0;
        Configuration conf = getConf();
        String[] otherArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (otherArgs.length != 2)
        {
            System.err.println("Usage: PageRank <graph> <outputdir>");
            System.exit(2);
        }

        /*
         * Create the page rank file which contains the initial rank values from
         * the graph input file
         */
        Path pageRankFile = createPageRankFile(otherArgs[0], otherArgs[1]);
        String pageRankOutput = otherArgs[1];
        Path outputFile = null;

        /*
         * Runs the page rank algorithm. This algorithm does not 
         * test for convergence it just runs for 15 iterations.
         */
        for (int i = 1; i <= 15 && result == 0; i++)
        {
            outputFile = new Path(pageRankOutput + i);
            result = rankPages(conf, pageRankFile, outputFile);
            pageRankFile = outputFile;

        }
        // Create the ranked output results
        conf = getConf();
        result = sortResults(conf, outputFile);
        return result;

    }

    /**
     * Creates a sorted result set from the ranked pages which is sorted by
     * rank and then url
     * {url}\t{rank}
     * 
     * @param conf
     * @param outputFile
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int sortResults(Configuration conf, Path outputFile)
            throws IOException, InterruptedException, ClassNotFoundException
    {
        int result;
        conf.set("mapred.text.key.comparator.options", "-k2nr -k1");
        conf.set("map.output.key.field.separator","\t");
        Job job = new Job(conf, "UrlSorter " + outputFile);
        job.setSortComparatorClass(KeyFieldBasedComparator.class);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(UrlSortMapper.class);
        job.setReducerClass(UrlSortReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, outputFile);
        FileOutputFormat.setOutputPath(job, outputFile.suffix("sorted"));

        result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }

    /**
     * Ranks the pages
     * 
     * @param conf
     * @param pageRankFile
     * @param outputFile
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private int rankPages(Configuration conf, Path pageRankFile, Path outputFile)
            throws IOException, InterruptedException, ClassNotFoundException
    {
        int result;
        Job job = new Job(conf, "Page Rank " + pageRankFile + " "
                + outputFile.getName());
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, pageRankFile);
        FileOutputFormat.setOutputPath(job, outputFile);

        result = job.waitForCompletion(true) ? 0 : 1;
        return result;
    }

    /**
     * Create the initial page rank file
     * [url]\t[initial rank],[url1],[url2],...,[urln]
     * 
     * @param in
     * @param out
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private Path createPageRankFile(String in, String out) throws IOException,
            InterruptedException, ClassNotFoundException
    {
        Path inPath = new Path(in);
        Path outPath = new Path(out + "0");

        double n = countLines(in, out);
        PageRankFileFormatMapper.setN(n);
        Configuration conf = getConf();
        Job job = new Job(conf, "PageRankFileFormatter " + in + " " + outPath);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(PageRankFileFormatMapper.class);
        job.setNumReduceTasks(0);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, inPath);
        FileOutputFormat.setOutputPath(job, outPath);

        job.waitForCompletion(true);

        return outPath;
    }

    /**
     * Counts the lines in the graph file
     * 
     * @param in
     * @param out
     * @return
     * @throws IOException
     * @throws InterruptedException
     * @throws ClassNotFoundException
     */
    private double countLines(String in, String out) throws IOException,
            InterruptedException, ClassNotFoundException
    {
        Configuration conf = getConf();

        Job job = new Job(conf, "Line Counter " + in + " " + out);
        job.setJarByClass(PageRank.class);
        job.setMapperClass(LineMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(in));
        FileOutputFormat.setOutputPath(job, new Path(out + "/count"));

        if (job.waitForCompletion(true))
        {
            Path countPath = new Path(out + "/count/part-r-00000");

            FSDataInputStream fdis = countPath.getFileSystem(conf).open(
                    countPath);
            byte[] is = new byte[1024];
            int b = fdis.read(is);
            String line = Bytes.toString(is, 0, b);
            StringTokenizer st = new StringTokenizer(line, "\t");
            st.nextToken();
            double count = Double.parseDouble(st.nextToken());
            return count;
        }
        return -1;
    }

    /**
     * Counts the number of lines in a file
     * 
     * @author val
     * 
     */
    public static class LineMapper extends
            Mapper<Object, Text, Text, IntWritable>
    {
        public static Text LINE = new Text("Lines");
        public static IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            context.write(LINE, ONE);
        }

    }

    /**
     * Creates the record to be sorted
     * 
     * @author val
     *
     */
    public static class UrlSortMapper extends
            Mapper<Object, Text, Text, Text>
    {
        private static Text KEY = new Text();
        private static Text NULL = new Text();
        private static String record=null;

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            StringTokenizer st = new StringTokenizer(value.toString(), "\t");
            record=st.nextToken();
            st = new StringTokenizer(st.nextToken(), ",");
            record+="\t"+st.nextToken();
            KEY.set(record);
            context.write(KEY,NULL );
        }
    }

    

    /**
     * Counts the number of lines in a file
     * 
     * @author val
     * 
     */
    public static class PageRankFileFormatMapper extends
            Mapper<Object, Text, Text, Text>
    {
        public static Text LINE = new Text();
        public static String url, newLine;
        public static StringTokenizer st;
        static double _n = 0;

        public static void setN(double n)
        {
            _n = n;
        }

        @Override
        protected void map(Object key, Text value, Context context)
                throws IOException, InterruptedException
        {
            System.out.println(value);
            st = new StringTokenizer(value.toString(), ",");
            url = st.nextToken();
            newLine = (double) 1 / _n + ",{";
            while (st.hasMoreTokens())
            {
                newLine += st.nextToken();
                newLine += (st.hasMoreTokens()) ? " " : "";
            }
            newLine += "}";
            context.write(new Text(url), new Text(newLine));
        }

    }
    

}
