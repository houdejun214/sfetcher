package com.sdata.core;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.text.ParseException;
import java.util.Date;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

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
       super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }
    
    public static void testOther()
    {
 		String patternStr = "(-?[1-9]\\d*\\,?\\d*\\.?\\d+)";
 		System.out.println(PatternUtils.getMatchPattern(patternStr, "sre123123",1));
 		System.out.println(PatternUtils.getMatchPattern(patternStr, "sre-123123",1));
 		System.out.println(PatternUtils.getMatchPattern(patternStr, "sre1231,23",1));
 		System.out.println(PatternUtils.getMatchPattern(patternStr, "sre12,312.3",1));
 		System.out.println(PatternUtils.getMatchPattern(patternStr, "sre1231a23",1));
    }
    
    /**
	 * 
	 * fetch next page
	 * 
	 * @param id
	 * @return
	 */
	private Document fetchPage(String url) {
		String content = HttpPageLoader.getDefaultPageLoader().download(url).getContentHtml();
		Document doc =Jsoup.parse(content);;
		return doc;
	}
	

	
//	long x = 3L;
//
	static String unescape(String s) {
		int i = 0, len = s.length();
		char c;
		StringBuffer sb = new StringBuffer(len);
		while (i < len) {
			c = s.charAt(i++);
			if (c == '\\') {
				if (i < len) {
					c = s.charAt(i++);
					if (c == 'u') {
						c = (char) Integer.parseInt(s.substring(i, i + 4), 16);
						i += 4;
					} // add other cases here as desired...
				}
			} // fall through: \ escapes itself, quotes any character but u
			sb.append(c);
		}
		return sb.toString();
	}

	private static DBCollection dbCollection;
	private static DB getMongoDB() throws UnknownHostException, MongoException{
		 System.out.println("Connecting to DB...");
		 Mongo mongo = new Mongo("172.18.109.20", 27017);
		 DB db = mongo.getDB("tencentOthers");
//		 db.authenticate("epl", "epl".toCharArray());
		 return db;
	}
	

	public static DBCursor query(int type){
		DBObject query = new BasicDBObject(Constants.OBJECT_ID,new BasicDBObject("$type",type));
		DBCursor cursor = dbCollection.find(query);
		return cursor;
	}
    /**
     * Rigourous Test :-)
     * @throws UnsupportedEncodingException 
     * @throws MongoException 
     * @throws UnknownHostException 
     * @throws ParseException 
     */
    public void testApp() throws UnsupportedEncodingException, UnknownHostException, MongoException, ParseException
    {
    	String text = "���������";
    	byte[] bytes = text.getBytes("UTF-8");
    	String string = new String(bytes,"UTF-8");
    	System.out.println(string);

		String s = DateTimeUtils.format(new Date(), "yyyy-MM-dd-HH");
		String e = DateTimeUtils.format(new Date(), "yyyy-MM-dd-HH");
		String r = "custom:".concat(s).concat(":").concat(e);
		String url = String.format("http://s.weibo.com/weibo/%s&xsort=time&timescope=custom:%s:%s&page=%d","1",DateTimeUtils.format(new Date(), "yyyy-MM-dd-HH"),DateTimeUtils.format(new Date(), "yyyy-MM-dd-HH"),2);
    }
}
