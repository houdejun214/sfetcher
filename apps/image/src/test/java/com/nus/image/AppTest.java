package com.nus.image;

import java.io.UnsupportedEncodingException;

import net.sf.json.JSONObject;

import com.lakeside.core.utils.PatternUtils;
import com.sdata.core.crawldb.CrawlDBImageQueueEnum;
import com.sdata.core.util.WebPageDownloader;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
	private static String separator = System.getProperty("file.separator");
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }
    
    public void testApp() throws UnsupportedEncodingException
    {
    	System.out.println(CrawlDBImageQueueEnum.SOURCE.value());

		JSONObject json  = new JSONObject();
		json.put("s", "www");
		json.put("v", "11111111");
		System.out.println(json.toString());
//    	String download = WebPageDownloader.download("http://news.sina.com.cn/o/2012-11-25/144925658189.shtml");
//    	System.out.println(download);
//		String charset = PatternUtils.getMatchPattern("HTTP-EQUIV=\"Refresh\".*URL=(.*)\"\\>",
//				download, 1);
//		System.out.println(charset);
		
    }
}
