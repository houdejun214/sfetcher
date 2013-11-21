package com.sdata.component.site;

import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSession;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import com.sdata.core.site.BaseDataApi;
import com.sdata.core.util.WebPageDownloader;

public class TestTwitterApi extends BaseDataApi {
	
	
	private static final String consumerKey="JxbZCJLzi4vQEJS7TNb9rA";
	private static final String consumerSecret="8Er6J6iI6EQH8jzYsbVrWbgdqFf3WcWBzsRE8Yxzu0";
	private static final String accessTokenName="544194759-esvNxnh4p8k8BAo95VteSplM5aVuPhCjQHEjuNuF";
	private static final String accessTokenSecret="ikNacleByw5U8O7SjF04DWZzTXQIMLLyFBoI4dobk";
	
	private static final String trendsRequestUrl="https://api.twitter.com/1/trends/1.json";
	private static final String tweetsInTrendsRequestUrl="https://twitter.com/phoenix_search.phoenix?include_entities=1&include_available_features=1&contributor_details=true";
	
	public static final int PerPage = 500;
	
	public TestTwitterApi(){
	}
	
	
	public static void main(String[] args) throws Exception {
		
//        Twitter twitter = new TwitterFactory().getInstance();
//        try {
//            Query query = new Query("ThatHeadThat");
//            query.setPage(10);
//			QueryResult result = twitter.search(query);
//            List<Tweet> tweets = result.getTweets();
//            for (Tweet tweet : tweets) {
//                System.out.println("@" + tweet.getFromUser() + " - " + tweet.getText());
//            }
//            System.exit(0);
//        } catch (TwitterException te) {
//            te.printStackTrace();
//            System.out.println("Failed to search tweets: " + te.getMessage());
//            System.exit(-1);
//        }
		
//		String content = WebPageDownloader.download(trendsRequestUrl);
//		System.out.println("********topics :"+content);
		
		
	    String tweetsUrl = tweetsInTrendsRequestUrl+"&q="+"%22Jorge%20Reyes%22"+"&X-Phx=true&count=200";
	    tweetsUrl = repairUrl(tweetsUrl);
		
	    for(int i = 0;i<5;i++){
	    	String tweetsContent =  WebPageDownloader.download(tweetsUrl);
	    	System.out.println("********tweetsInTrends :"+tweetsContent);
	    }
		
	}
	
	private static String repairUrl(String link){
		if(link==null){
			return "";
		}
		link = link.replaceAll(" ", "%20");
		link = link.replaceAll("#", "%23");
		link = link.replaceAll("\"", "%22");
		return link;
	}
	
	
	
}

