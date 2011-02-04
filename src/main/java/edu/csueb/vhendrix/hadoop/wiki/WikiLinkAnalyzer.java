package edu.csueb.vhendrix.hadoop.wiki;
//###########################################################################
//Val Hendrix
//CS6580 - Distributed systems
//Extra Credit Assignment PageRank
//Due: December 2, 2010
//##########################################################################

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * This class pulls out the links in the source of a media wiki page
 */
public class WikiLinkAnalyzer
{

    private Set<String> links = new TreeSet<String>(String.CASE_INSENSITIVE_ORDER);

    /**
     * @param source
     *            - file containing source code
     */
    public void initialize(File source)
    {
        try
        {
            Reader r = new BufferedReader(new FileReader(source));
            initialize(r);
        } catch (FileNotFoundException e)
        {
            e.printStackTrace();
            System.exit(0);
        }
    }

    public void initialize(String source)
    {
        Reader r = new BufferedReader(new StringReader(source));
        initialize(r);
    }

    protected void initialize(Reader r)
    {
        StreamTokenizer st = new StreamTokenizer(r);

        // ignore comments
        st.slashSlashComments(true);

        
        // treat these as ordinary characters
        st.ordinaryChar('-');
        st.ordinaryChar('.');
        st.ordinaryChar('1');
        st.ordinaryChar('2');
        st.ordinaryChar('3');
        st.ordinaryChar('4');
        st.ordinaryChar('5');
        st.ordinaryChar('6');
        st.ordinaryChar('7');
        st.ordinaryChar('8');
        st.ordinaryChar('9');
        st.ordinaryChar('0');
        st.ordinaryChar('[');
        st.ordinaryChar(']');
        tokenizer = st;
    }

    /**
     * Prints a summary of the lexical analysis
     */
    public void print()
    {

        System.out.print("Links:");
        Iterator<String> itr = links.iterator();
        while (itr.hasNext())
        {
            System.out.print(itr.next() + "  ");
        }
        System.out.println("\n");

    }

    /**
     * 
     * @return wiki links
     */
    public Set<String> getLinks()
    {
        return links;
    }

    /**
     * Lexically analyzes the source file
     * 
     * @throws IOException
     */
    public void parse() throws IOException
    {

        StringBuffer link = new StringBuffer();

        /*
         * Collect the tokens, determine their class and log the number of times
         * each token class is encountered
         */
        while (tokenizer.nextToken() != StreamTokenizer.TT_EOF)
        {
            int type = tokenizer.ttype;

            switch (type)
            {
            case StreamTokenizer.TT_EOF: // EE_EOF should never be encountered
            case StreamTokenizer.TT_EOL: // nothing special about the endo of
                                         // line

                break;
            case StreamTokenizer.TT_WORD:
                if (link.length() > 0)
                {
                    link.append(tokenizer.sval);
                }

                break;
            default:
                // special characters
                char c = (char) type;
                if ((c == '[' && link.length() == 0))
                {
                    tokenizer.nextToken();
                    c = (char) tokenizer.ttype;
                    if (c == '[')
                        link.append(c);
                } else if (c == ']' && link.length() > 0)
                {
                    tokenizer.nextToken();
                    if (c == ']')
                    {
                        links.add(link.substring(1, link.length()).trim());
                        link.delete(0, link.length()); // reset the link
                    }
                } else if (link.length() > 0 && (c == ':' || c=='#'))
                {
                    link.delete(0, link.length()); // reset the link
                } else if (link.length() > 0 && c == '|')
                {
                    links.add(link.substring(1, link.length()).trim());
                    link.delete(0, link.length()); // reset the link
                } else if (link.length() > 0)
                {
                    link.append(c);

                }

            }

        }
    }

    /**
     * Driver for this class
     * 
     * @param args
     */
    public static void main(String[] args)
    {

        if (args.length < 1)
        {
            System.out.println("Usage: " + WikiLinkAnalyzer.class.getName()
                    + " <source-code>");
            System.exit(0);
        }

        File source = new File(args[0]);
        if (!source.isFile())
        {
            System.err.println("This is not a valid file");
            System.exit(0);
        }

        WikiLinkAnalyzer la = new WikiLinkAnalyzer();
        la.initialize(source);
        try
        {
            la.parse();
        } catch (IOException e)
        {
            e.printStackTrace();
        }

        la.print();

    }

    private StreamTokenizer tokenizer;

}
