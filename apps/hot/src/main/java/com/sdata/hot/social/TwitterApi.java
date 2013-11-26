package com.sdata.hot.social;

import java.util.List;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;

import com.lakeside.download.http.HttpPageLoader;
import com.sdata.core.Configuration;
import com.sdata.core.site.BaseDataApi;

/**
 * @author zhufb
 *
 */
public class TwitterApi extends BaseDataApi {
	
	private final String SearchUrl ="https://twitter.com/i/search/timeline?q=%s&src=typd&mode=media&include_available_features=1&include_entities=1";
	
	private Twitter twitter = null;
	
	public TwitterApi(Configuration conf){
		twitter = new TwitterFactory().getInstance();
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String AccessToken = conf.get("AccessToken");
		String AccessTokenSecret = conf.get("AccessTokenSecret");
		twitter.setOAuthConsumer(consumerKey, consumerSecret);
		AccessToken accessToken = new AccessToken(AccessToken,AccessTokenSecret);
		twitter.setOAuthAccessToken(accessToken);
	}
	
	/**
	 * search by query word
	 * 
	 * @param word
	 * @return
	 */
	public List<Status> search(String word)  {
		try{
			return this.search(word,3);
		}catch (TwitterException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * search by query word
	 * 
	 * @param word
	 * @return
	 */
	public List<Status> searchRecent(String word)  {
		try{
			return this.searchRecent(word,3);
		}catch (TwitterException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	/**
	 * search by query word and count
	 * 
	 * @param word
	 * @param count
	 * @return
	 * @throws TwitterException
	 */
	public List<Status> search(String word,int count) throws TwitterException{
		Query query = new Query(word);
		query.setCount(count);
		query.setLang("en");
		query.setResultType(Query.POPULAR);
		QueryResult result = twitter.search(query);
		return result.getTweets();
	}
	
	/**
	 * search by query word and count
	 * 
	 * @param word
	 * @param count
	 * @return
	 * @throws TwitterException
	 */
	public List<Status> searchRecent(String word,int count) throws TwitterException{
		Query query = new Query(word);
		query.setCount(count);
		query.setLang("en");
		query.setResultType(Query.RECENT);
		QueryResult result = twitter.search(query);
		return result.getTweets();
	}

	public Trends trends(int id) {
		try{
			return twitter.getPlaceTrends(id);
		}catch(TwitterException e){
			e.printStackTrace();
		}
		return null;
	}
	
	/**search by word with url
	 * 
	 * @param word 
	 * @return
	 */
	public String searchByUrl(String word){
		String url = String.format(SearchUrl, word);
		return HttpPageLoader.getDefaultPageLoader().download(url).getContentHtml();
	}
}

