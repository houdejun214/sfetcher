package com.sdata.core;

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.X509Certificate;
import java.util.Date;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;

import com.lakeside.core.utils.time.DateTimeUtils;

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
    
    public void shouldAcceptUnsafeCerts() throws Exception {
        DefaultHttpClient httpclient = httpClientTrustingAllSSLCerts();
        HttpGet httpGet = new HttpGet("https://host_with_self_signed_cert");
        HttpResponse response = httpclient.execute( httpGet );
        assertEquals("HTTP/1.1 200 OK", response.getStatusLine().toString());
    }

    private static DefaultHttpClient httpClientTrustingAllSSLCerts() throws NoSuchAlgorithmException, KeyManagementException {
        DefaultHttpClient httpclient = new DefaultHttpClient();
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, getTrustingManager(), new java.security.SecureRandom());
        SSLSocketFactory socketFactory = new SSLSocketFactory(sc);
        Scheme sch = new Scheme("https", 443, socketFactory);
        httpclient.getConnectionManager().getSchemeRegistry().register(sch);
        return httpclient;
    }

    private static TrustManager[] getTrustingManager() {
        TrustManager[] trustAllCerts = new TrustManager[] { new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
                // Do nothing
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
                // Do nothing
            }

        } };
        return trustAllCerts;
    }
    
  
    public static void testApp() throws Exception { 

    	Date date = DateTimeUtils.getBeginOfDay(new Date());
    	long unixTime = DateTimeUtils.getUnixTime(date);
    	System.out.println(unixTime);
    	System.out.println(date.getTime());
    	
    	
//    	Map<String,String> map = new HashMap<String,String>();
//		String cookie=	Weibo.getCookie();
////    	System.out.print(cookie);
//    	map.put("Cookie", cookie);
//    	for(int i = 1;i<=3;i++){
//    		//
////    		        	WeiboServer.init("weibo".concat(String.valueOf(i)));
////    		    		http://www.weibo.com/aj/mblog/info/big?_wv=5&id=3628646650122660&max_id=3628993855063435&page=76&__rnd=1380703377204
////    			    	String url = "http://www.weibo.com/aj/mblog/info/big?_wv=5&id=3628646650122660&max_id=3628993855063435&page=76&__rnd=1380703377204";
//    			    	String url = "http://www.weibo.com/2075636451/Ac7A16cDW";
////    			    	String download = WebPageDownloader.download(url, map);
//    		    		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(map,url);
//    			    	System.out.println(page.getContentHtml());
//    		    	}
//    	Set<String> set = new HashSet<String>();
//    	set.add("A");
//    	set.add("B");
//    	set.add("C");
//    	set.add("D");
//    	set.add("E");
//
//    	int i = 0;
//    	while(i<10){
//	    	Iterator<String> iterator = set.iterator();
//	    	while(iterator.hasNext()){
//				System.out.print(iterator.next().toString());
//	    	}
//    	}
//    	
//    	Document document = DocumentUtils.getDocument("https://twitter.com/marinabaysands");
//		if(document == null){
//			return;
//		}
//		String tweets = JsoupUtils.getText(document, ".stats a.js-nav:contains(Tweets) strong");
//		String following = JsoupUtils.getText(document, ".stats a.js-nav:contains(Following) strong");
//		String followers = JsoupUtils.getText(document, ".stats a.js-nav:contains(Followers) strong");
//		System.out.println(Integer.valueOf(tweets.replaceAll(",","")));
//		System.out.println(Integer.valueOf(following.replaceAll(",","")));
//		System.out.println(Integer.valueOf(followers.replaceAll(",","")));
//		
//    	Pattern DatePattern7 = Pattern.compile("([A-Za-z]*\\s{1}\\d{1,2},\\s{1}\\d{4})");
////    	HttpPage download = HttpPageLoader.getDefaultPageLoader().download("http://www.google.com/uds/GblogSearch?callback=google.search.BlogSearch.RawCompletion&rsz=small&hl=en&source=gsc&gss=.sg&sig=cf717ce13f86fb2ebeed8f0046aba6ef&q=jumbo%20blogurl%3Ahttp%3A%2F%2Fieatishootipost.sg%2F&qid=141358863d05734e7&context=0&key=notsupplied&v=1.0&nocache=1379582613079&start=0");
//    	String input = "August 30, 2013 |";
////    	String reg = "\\{.*?\\}";
//    	Matcher matcher = DatePattern7.matcher(input);
//		boolean find = matcher.find();
//    	if(find){
//        	System.out.print(matcher.group(1));
//    	}

//    	
//    	String dateStr = " August 26, 2013 ";
//    	Date d = new Date();
//    	SimpleDateFormat Format6 = new SimpleDateFormat("MMMMM dd, yyyy",Locale.ENGLISH);
//    	System.out.print(Format6.format(d));
//		Date date = Format6.parse(dateStr.trim());
//		
//    	System.out.print(date);
//    	1087770692
//    	HttpPage download = HttpPageLoader.getDefaultPageLoader().download("http://danielfooddiary.com/?s=jumbo");
//    	System.out.print(download.getContentHtml());
//    	long mid2Id = WeiboHelper.mid2Id("sHZwBCdd33jZxr0gTxhl9w==");
//    	String replaceMatchGroup = PatternUtils.replaceMatchGroup("page=(\\d)&", "http://data.weibo.com/top/hot/all?page=2&class=all&depart=all", 1, String.valueOf(30));
//    	System.out.println(replaceMatchGroup);
//    	
//		byte[] base64Decode = EncodeUtils.base64Decode("sHZwBCdd33jZxr0gTxhl9w==");
//		ByteBuffer wrap = ByteBuffer.wrap(base64Decode);
//		wrap.getInt();
////		int long1 = wrap.getShort();
//		System.out.println(new String(base64Decode,"GBK"));
//		long x = new Long(base64Decode);
		
    }
}
