package com.sdata.component.site;

import com.sdata.core.site.BaseDataApi;


public class TwitterTopicApi extends BaseDataApi {
	
	// example:https://api.twitter.com/1/trends/1.json
	private static final String trendsRequestUrl="https://twitter.com/trends?woeid=";
//	private static final String tweetsInTrendsRequestUrl="https://api.twitter.com/1.1/search/tweets.json?";
	
	public TwitterTopicApi(){
	}
	
	/**
	 * get search topics url with topicLocation and tweetCount
	 * @param topicLocation
	 * @param tweetCount
	 * @return
	 * @author qiumm
	 */
	public String getSearchTopicsUrl(String topicLocation){
		if(topicLocation==null || topicLocation.equals("") ){
			throw new RuntimeException("");
		}
		String queryUrl=trendsRequestUrl+topicLocation;
		return repairLink(queryUrl);
	}
	
	
//	public String getTweetsInTopicUrl(String topicName,String tweetCount){
//		if(tweetCount==null || tweetCount.equals("") ){
//			throw new RuntimeException("");
//		}
//		String queryUrl=tweetsInTrendsRequestUrl+"&q="+topicName+"&count=200";
//		return repairLink(queryUrl);
//	}
	
}

