//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix.hadoop.wiki;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import edu.csueb.vhendrix.hadoop.wiki.WikiDumpToLinkGraph;

public class WikiDumpToLinkGraphTest
{

    private static final String PART_R_00000 = "part-r-00000";

    @Test
    public void testRun() throws Exception
    {
        Configuration conf = new Configuration();
        conf.set("fs.default.name", "file:///");
        conf.set("mapred.job.tracker", "local");

        Path input = new Path("target/test-classes/data/enwiki/in");
        Path output = new Path("target/test-classes/data/enwiki/out");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true); // delete old output

        WikiDumpToLinkGraph driver = new WikiDumpToLinkGraph();
        driver.setConf(conf);

        int exitCode = driver.run(new String[]
        { input.toString(), output.toString() });
        assertThat(exitCode, is(0));
        List<String> check = new ArrayList<String>();
        check.add("AccessibleComputing");
        check.add("AfghanistanCommunications");
        check.add("AfghanistanGeography");
        check.add("AfghanistanHistory");
        check.add("AfghanistanMilitary");
        check.add("AfghanistanPeople");
        check.add("AfghanistanTransnationalIssues");
        check.add("AfghanistanTransportations");
        check.add("AlbaniaEconomy");
        check.add("AlbaniaGovernment");
        check.add("AlbaniaHistory");
        check.add("AlbaniaPeople");
        check.add("AllSaints");
        check.add("AmoeboidTaxa");
        check.add("Anarchism");
        check.add("AsWeMayThink");
        check.add("AssistiveTechnology");
        check.add("Autism");
        checkOutput(conf, new Path(output.toString()), check);
    }

    private void checkOutput(Configuration conf, Path output,
            List<String> check) throws IOException
    {
        FileSystem fs = FileSystem.getLocal(conf);

        assertThat(fs.exists(output), is(true));

        Path results = new Path(output, PART_R_00000);
        assertThat(fs.exists(results), is(true));

        FSDataInputStream is = fs.open(results);
        BufferedReader br = new BufferedReader(new InputStreamReader(is));
        String line = null;
        int i=0;
        while ((line = br.readLine()) != null)
        {
            line.startsWith(check.get(i));
            i++;
        }   
        assertThat(i, is(check.size()));
        
    }
}
