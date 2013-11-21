package com.sdata.component.site;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.Authorization;
import twitter4j.internal.http.HttpParameter;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.site.BaseDataApi;
import com.sdata.core.util.WebPageDownloader;

public class TwitterApi extends BaseDataApi {
	
	private static final Object syncObject = new Object();
	private static final String SearchUrl ="http://search.twitter.com/search.json";
	
	private TwitterStream twitterStream = null;
	private Twitter twitter = null;
	private Queue<Map> queue = null;
	
	protected Authorization auth;
	public TwitterApi(){
		twitterStream = new TwitterStreamFactory().getInstance();
		twitter = new TwitterFactory().getInstance();
		System.setProperty("twitter4j.debug", "false");
		queue = new ArrayBlockingQueue <Map>(1000);
	}
	
	/**
	 * 设置身份验证
	 * @param consumerKey
	 * @param consumerSecret
	 * @param accessTokenName
	 * @param accessTokenSecret
	 * @author qiumm
	 */
	public void setOAuth(String consumerKey, String consumerSecret,String accessTokenName,String accessTokenSecret){
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitter.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken accessToken = new AccessToken(accessTokenName,accessTokenSecret);
		twitterStream.setOAuthAccessToken(accessToken);
		twitter.setOAuthAccessToken(accessToken);
	}
	
	public Map<String,Object> getStatus(Long id) throws TwitterException{
		Status status = twitter.showStatus(id);
		return status.getJSONObject().toMap();
	}
	
	/**
	 * startSample -----ready for start twitterStream
	 * @param listener
	 * @throws Exception
	 * @author qiumm
	 */
	public void startSample(){
		try {
			// define listener
			StatusListener listener = new StatusListener() {
		        public void onStatus(Status status) {
		        	//对获取到的twitter进行处理
		        	synchronized (syncObject) {
		        		queue.offer(status.getJSONObject().toMap());
		        	}
		        	//System.out.println(status.getJSONObject());
		        }

				public void onException(Exception ex) {
		            ex.printStackTrace();
		        }

				public void onDeletionNotice(StatusDeletionNotice arg0) {
					// nothings to do
					
				}

				public void onScrubGeo(long arg0, long arg1) {
					//  nothings to do
					
				}

				public void onTrackLimitationNotice(int arg0) {
					//  nothings to do
					
				}

				public void onStallWarning(StallWarning warning) {
					// TODO Auto-generated method stub
					
				}
		    };
			twitterStream.addListener(listener);
			twitterStream.sample();
			log.info("**************************  twitterStream has be start  *****************************************");
		} catch (Exception e) {
			log.info("Error information with startSample ["+e.getMessage()+"]");
			return;
		}
	}
	
	/**
	 * startSample -----ready for start twitterStream filter
	 * @param listener
	 * @throws Exception
	 * @author qiumm
	 */
	public void startFilter(FilterQuery query){
		try {
			// define listener
			StatusListener listener = new StatusListener() {
		        public void onStatus(Status status) {
		        	//对获取到的twitter进行处理
		        	synchronized (syncObject) {
		        		queue.offer(status.getJSONObject().toMap());
		        	}
//		        	System.out.println(status.getJSONObject());
		        }

				public void onException(Exception ex) {
		            ex.printStackTrace();
		        }

				public void onDeletionNotice(StatusDeletionNotice arg0) {
					// nothings to do
					
				}

				public void onScrubGeo(long arg0, long arg1) {
					//  nothings to do
					
				}

				public void onTrackLimitationNotice(int arg0) {
					//  nothings to do
					
				}

				public void onStallWarning(StallWarning warning) {
					// TODO Auto-generated method stub
					
				}

		    };
			twitterStream.addListener(listener);
			twitterStream.filter(query);
			log.info("**************************  twitterStream(filter) has be start  *****************************************");
		} catch (Exception e) {
			log.info("Error information with startSample ["+e.getMessage()+"]");
			return;
		}
	}
	
	/**
	 * transform and deal the tweets that get from twitterStream
	 * 
	 * @author qiumm
	 */
	public List<Map> getTweets(){
		while(queue.size() < 50 ){
			try {
				Thread.currentThread().sleep(200);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		int len = queue.size();
		List<Map> list= new ArrayList<Map>();
		if(len>0){
			for(int i=0;i<len;i++){
				list.add(queue.poll());
			}
		}
		return list;
	}
	
	
	public String dateToString(Date time){
		return String.valueOf(time.getTime()/1000);
		//return DateTimeUtils.format(time, "yyyy-MM-dd HH:mm:ss");
	}
	
	/**
	 * search by query
	 * 
	 * @param queryWords
	 * @param page
	 * @return
	 * @throws TwitterException 
	 */
	public String search(String queryWords,int page, long sinceId)  {
		try{

			return this.search(queryWords, page, sinceId, -1);
		}catch (TwitterException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * search by query
	 * 
	 * @param queryWords
	 * @param page
	 * @param sinceId
	 * @param maxId
	 * @return
	 * @throws TwitterException 
	 */
	public String search(String queryWords,int page, long sinceId,long maxId) throws TwitterException{

		Query query = new Query(queryWords);
		query.setSinceId(sinceId);
		query.setMaxId(maxId);
		query.setCount(100);
		query.setLang("en");
		QueryResult result = twitter.search(query);
		return result.asString();
		
//		StringBuilder url = new StringBuilder(SearchUrl);
//		appendParameter("q", queryWords, url);
//        appendParameter("lang", "en", url);
//        appendParameter("max_id", maxId, url);
//        appendParameter("rpp", 100, url);
//        appendParameter("page", page, url);
//        appendParameter("since_id", sinceId, url);
//        
//		String content = httpGet(url.toString());
//		return content;
	}
	

	public List<Status> search(String word,int count) {
		try{
			Query query = new Query(word).count(count);
			QueryResult result = twitter.search(query);
			return result.getTweets();
		}catch(TwitterException e){
			e.printStackTrace();
		}
		return null;
	}
	

	public Trends trends(int id) {
		try{
			return twitter.getPlaceTrends(id);
		}catch(TwitterException e){
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * search by url
	 * @param url
	 * @return
	 */
	public String searchByUrl(String queryurl){
		StringBuilder url = new StringBuilder(SearchUrl+queryurl);
		return httpGet(url.toString());
	}
	
	private String httpGet(String url){
		int count=0;
		while(true){
			String content = WebPageDownloader.download(url);
			if(StringUtils.isNotEmpty(content)){
				return content;
			}
			count++;
			if(count>=5){
				return "";
			}
			log.warn("twitter search exception try for again [{}]",count);
			this.await(10000);
		}
	}
	
	public QueryResult search(Query query){
		QueryResult result;
		try {
			result = twitter.search(query);
		} catch (TwitterException e) {
			result = null;
		}
		return result;
	}
	
	protected void appendParameter(String name, String value, StringBuilder url) {
		if (value != null) {
			if(url.indexOf("?")<0){
				url.append("?");
			}else{
				url.append("&");
			}
			url.append(name + "=" + HttpParameter.encode(value));
		}
	}

	protected void appendParameter(String name, int value, StringBuilder url) {
		if (value >= 0) {
			if(url.indexOf("?")<0){
				url.append("?");
			}else{
				url.append("&");
			}
			url.append(name + "=" + value);
		}
	}

	protected void appendParameter(String name, long value, StringBuilder url) {
		if (value >= 0) {
			if(url.indexOf("?")<0){
				url.append("?");
			}else{
				url.append("&");
			}
			url.append(name + "=" + value);
		}
	}
	
	protected void await(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}

