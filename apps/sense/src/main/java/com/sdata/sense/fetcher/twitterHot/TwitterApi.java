package com.sdata.sense.fetcher.twitterHot;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import twitter4j.FilterQuery;
import twitter4j.Paging;
import twitter4j.ResponseList;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.auth.Authorization;

/**
 * @author zhufb
 *
 */
public class TwitterApi{
	private static final Object syncObject = new Object();
	private TwitterStream twitterStream = null;
	private Twitter twitter = null;
	private Queue<Map> queue = null;
	protected Authorization auth;
	public TwitterApi(String consumerKey, String consumerSecret,String accessTokenName,String accessTokenSecret){
		twitterStream = new TwitterStreamFactory().getInstance();
		twitter = new TwitterFactory().getInstance();
		queue = new ArrayBlockingQueue <Map>(1000);
		twitterStream.setOAuthConsumer(consumerKey, consumerSecret);
		twitter.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken accessToken = new AccessToken(accessTokenName,accessTokenSecret);
		twitterStream.setOAuthAccessToken(accessToken);
		twitter.setOAuthAccessToken(accessToken);
	}
	
	public List<Map<String,Object>> getUserTimeline(Long uid) throws TwitterException{
		return getUserTimeline(uid,0L);
	}

	public List<Map<String,Object>> getUserTimeline(Long uid,Long sinceId) throws TwitterException{
		Paging page = new Paging();
		if(sinceId != 0){
			page.setSinceId(sinceId);
		}
	    ResponseList<Status> userTimeline = twitter.getUserTimeline(uid,page);
	    List<Map<String,Object>> result = new ArrayList<Map<String,Object>>();
	    for(Status status:userTimeline){
	    	result.add(status.getJSONObject().toMap());
	    }
		return result;
	}
	
	/**
	 * startFilter -----ready for start twitterStream filter
	 * @param listener
	 * @throws Exception
	 * @author qiumm
	 */
	public void startFilter(FilterQuery query){
		try {
			// define listener
			StatusListener listener = new StatusListener() {
		        public void onStatus(Status status) {
		        	//瀵硅幏鍙栧埌鐨則witter杩涜澶勭悊
		        	synchronized (syncObject) {
		        		queue.offer(status.getJSONObject().toMap());
		        	}
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
		} catch (Exception e) {
			return;
		}
	}
	
	public List<Map> getTweets(){
		while(queue.size() < 20 ){
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
	
	public void stop(){
		this.twitterStream.shutdown();
	}
}

