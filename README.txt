This is how you can run the examples for this project

1) Build the jar from the sources
    a) You first need to download hadoop 0.2.0 from http://hadoop.apache.org and unpack it. 
       Once you have done that you need to set the envrionment variable HADDOP_HOME to 
       where you unpacked it to.
    b) Install apache maven (http://maven.apache.org)
    c) At the command line go the the directory of this tutorial project and type the following command
        > mvn test hadoop:pack
       this will run all of the unit tests and if they are successful the hadoop enables jar file will be
       built. The test results and input and output data can be found in the target/surefire-reports and 
       target/test-classes/data directorys respectively.
    d) The jar is now available in {project-dir}/target/hadoop-deploy/
    
2) Use the built jar file to test  out the tools. 
For instructions on how to use the MailCounter or WordCount tools run the tool with 
no arguments like below.

 PageRank
    > hadoop jar {project-dir}/target/hadoop-deploy/mapreduce-tools-hdeploy.jar edu.csueb.vhendrix.hadoop.pagerank.PageRank
    
 WordCount
    > hadoop jar {project-dir}/target/hadoop-deploy/mapreduce-tools-hdeploy.jar edu.csueb.vhendrix.hadoop.wordcount.WordCount
    
 WikiDumpToLinkGraph
    > hadoop jar {project-dir}/target/hadoop-deploy/mapreduce-tools-hdeploy.jar edu.csueb.vhendrix.hadoop.wiki.WikiDumpToLinkGraph
    

All tools are configured tools which uses hadoop GenericOptionsParser 
which also allows you to set individual properties. 

For example:
      > hadoop jar {project-dir}/target/hadoop-deploy/hadoop-tutorial-hdeploy.jar edu.csueb.vhendrix.hadoop.wordcount.WordCount\
       -D mapred.reduce.tasks=n in/ out/

The -D option is used to number of map reduces tasks for you job. The -D options take priority 
over properties from the configuration files.  (see "Hadoop Definitive Guide, 1st Edition", pg 122)


    
    
    

