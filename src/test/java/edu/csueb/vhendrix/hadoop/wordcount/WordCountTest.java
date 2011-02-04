//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop.wordcount;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import edu.csueb.vhendrix.hadoop.wordcount.WordCount;

public class WordCountTest
{

    @Test
    public void testWordCounter() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        Path input = new Path("target/test-classes/data/wcount/in/");
        Path output = new Path("target/test-classes/data/wcount/out/");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output

        WordCount driver = new WordCount();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]
        { input.toString(), output.toString() });
        assertThat(exitCode, is(0));
        Map<String, String> check = new HashMap<String, String>();
        check.put("america", "1");
        check.put("blessings", "1");
        check.put("common", "1");
        check.put("constitution", "1");
        check.put("defence", "1");
        check.put("do", "1");
        check.put("domestic", "1");
        check.put("establish", "2");
        check.put("form", "1");
        check.put("general", "1");
        check.put("insure", "1");
        check.put("justice", "1");
        check.put("liberty", "1");
        check.put("more", "1");
        check.put("ordain", "1");
        check.put("order", "1");
        check.put("our", "1");
        check.put("ourselves", "1");
        check.put("people", "1");
        check.put("perfect", "1");
        check.put("posterity", "1");
        check.put("promote", "1");
        check.put("provide", "1");
        check.put("secure", "1");
        check.put("states", "2");
        check.put("tranquility", "1");
        check.put("union", "1");
        check.put("united", "2");
        check.put("we", "1");
        check.put("welfare", "1");

        checkOutput(conf, output, check);
    }

    @Test
    public void testWordCounterParseSkipFile() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");
        conf.set("wordcount.skip.patterns", "true");
        conf.set("mapred.cache.localFiles",
                "target/test-classes/data/parseSkipFile.txt");

        Path input = new Path("target/test-classes/data/wcount/in/");
        Path output = new Path("target/test-classes/data/wcount/out/");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output

        WordCount driver = new WordCount();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]
        { input.toString(), output.toString() });
        assertThat(exitCode, is(0));
        Map<String, String> check = new HashMap<String, String>();
        check.put("america", "1");
        check.put("blessings", "1");
        check.put("common", "1");
        check.put("constitution", "1");
        check.put("defence", "1");
        check.put("do", "1");
        check.put("domestic", "1");
        check.put("establish", "2");
        check.put("form", "1");
        check.put("general", "1");
        check.put("insure", "1");
        check.put("justice", "1");
        check.put("liberty", "1");
        check.put("more", "1");
        check.put("ordain", "1");
        check.put("order", "1");
        check.put("our", "1");
        check.put("ourselves", "1");
        check.put("people", "1");
        check.put("perfect", "1");
        check.put("posterity", "1");
        check.put("promote", "1");
        check.put("provide", "1");
        check.put("secure", "1");
        check.put("states", "2");
        check.put("tranquility", "1");
        check.put("union", "1");
        check.put("united", "2");
        check.put("we", "1");
        check.put("welfare", "1");
        checkOutput(conf, output, check);
    }

    private void checkOutput(Configuration conf, Path output,
            Map<String, String> c) throws IOException
    {
        FileSystem fs = FileSystem.getLocal(conf);

        assertThat(fs.exists(output), is(true));

        Path results = new Path(output, "part-r-00000");
        assertThat(fs.exists(results), is(true));

        FSDataInputStream is = fs.open(results);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line = null;
        Map<String, String> r = new HashMap<String, String>();
        while ((line = br.readLine()) != null)
        {
            String[] v = line.split("\t");
            r.put(v[0], v[1]);
        }
        assertThat(r.size(), is(c.size()));
        assertThat(r, is(c));
    }
}
