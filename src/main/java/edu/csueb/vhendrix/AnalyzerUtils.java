//###########################################################################
// Val Hendrix
// CS6580 - Distributed systems
// Assignment WordCooccurrences
// Due: November 23, 2010
//##########################################################################
package edu.csueb.vhendrix;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Token;
import org.apache.lucene.analysis.TokenStream;

/**
 * Utilities for Lucene Analyzers
 * 
 * @author vhendrix
 * 
 */
public final class AnalyzerUtils
{
    /**
     * @param analyzer
     * @param text
     * @return array of tokens for the specified text using the given Analyzer
     * @throws IOException
     */
    public static Token[] tokensFromAnalysis(Analyzer analyzer, String text)
            throws IOException
    {
        if (text == null || text.length() == 0)
            return new Token[0];

        return tokensFromAnalysis(analyzer,new StringReader(text));
    }
    
    /**
     * @param analyzer
     * @param text
     * @return array of tokens for the specified text using the given Analyzer
     * @throws IOException
     */
    public static String tokenStringFromAnalysis(Analyzer analyzer, String text)
            throws IOException
    {
        if (text == null || text.length() == 0)
            return null;

        return tokenStringFromAnalysis(analyzer,new StringReader(text));
    }
    
    /**
     * @param analyzer
     * @param text
     * @return array of tokens for the specified text using the given Analyzer
     * @throws IOException
     */
    public static Token[] tokensFromAnalysis(Analyzer analyzer, Reader reader)
            throws IOException
    {

        TokenStream stream = analyzer.tokenStream("contents", reader);
        ArrayList<Token> tokenList = new ArrayList<Token>();
        while (true)
        {
            Token token = stream.next();
            if (token == null)
                break;
            tokenList.add(token);
        }
        return (Token[]) tokenList.toArray(new Token[0]);
    }
    
    /**
     * @param analyzer
     * @param text
     * @return array of tokens for the specified text using the given Analyzer
     * @throws IOException
     */
    public static String tokenStringFromAnalysis(Analyzer analyzer, Reader reader)
            throws IOException
    {

        TokenStream stream = analyzer.tokenStream("contents", reader);
        StringBuffer tokenBuf = new StringBuffer();
        while (true)
        {
            Token token = stream.next();
            if (token == null)
                break;
			if (token.termText().equalsIgnoreCase("i")
					|| token.termText().length() > 1) {
				tokenBuf.append(token.termText());
				tokenBuf.append(" ");
			}
        }
        return tokenBuf.toString();
    }

    /**
     * @param analyzer
     * @param texts array of texts to tokenize
     * @return array of tokens for the specified text using the given Analyzer
     * @throws IOException
     */
    public static Token[] tokensFromAnalysis(Analyzer analyzer, String[] texts)
            throws IOException
    {
        if (texts == null || texts.length == 0)
            return new Token[0];

        ArrayList<Token> tokenList = new ArrayList<Token>();
        for (int i = 0; i < texts.length; i++)
        {
            TokenStream stream = analyzer.tokenStream("contents",
                    new StringReader(texts[i]));
            while (true)
            {
                Token token = stream.next();
                if (token == null)
                    break;
                tokenList.add(token);
            }
        }
        return (Token[]) tokenList.toArray(new Token[0]);
    }

}
