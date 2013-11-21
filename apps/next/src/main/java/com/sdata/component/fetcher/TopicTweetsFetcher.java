package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import weibo4j.Weibo;
import weibo4j.util.WeiboServer;

import com.lakeside.core.utils.StringUtils;
import com.sdata.component.parser.TopicTweetsParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;

/**
 * Weibo topic info fetcher implement
 * 
 * @author zhufb
 *
 */
public class TopicTweetsFetcher extends SdataFetcher{
	
	
	public static final Log log = LogFactory.getLog("SdataCrawler.TopicTweetsFetcher");
	
	public TopicTweetsFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
	    this.parser =  new TopicTweetsParser(conf,state);
	  //init token
  		Weibo.initToken();
  		String token = conf.get("token");
  		if(StringUtils.isEmpty(token)){
  			token = CrawlAppContext.state.getCrawlName();
  		}
		WeiboServer.init(token);
	}

	/* 
	 * fetch topic list info 
	 *
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatumList()
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		log.fatal("start initFtlTopicList,startTime:"+(new Date()).toString());
		return ((TopicTweetsParser)parser).initFtlTopicList();
	}
	
	/* 
	 * fetch one topic detail contains description and tweet list under this topic
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatum(com.sdata.component.FetchDatum)
	 */
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return fetchTopicTweets(datum);
	}

	/**
	 * Parse json data to datum 
	 * @param JSONObject json
	 * @return FetchDatum
	 */
	public FetchDatum fetchTopicTweets(FetchDatum datum) {
		Map<String, Object> topic = datum.getMetadata();
		log.fatal("fetchTopicTweets,startTime:"+(new Date()).toString());
		@SuppressWarnings("unchecked")
		List<String> currList= (List<String>)topic.get(Constants.TOPIC_TWEETS_CURR_FETCHED_LIST); 
		List<JSONObject> tweets = new ArrayList<JSONObject>();
		for(int i=0;i<currList.size();i++){
			try {
				//get tweets
				JSONObject tw = ((TopicTweetsParser)parser).fetchTweet(currList.get(i));
				tweets.add(tw);
			}catch(RuntimeException e){
				continue;
			}
		}
		topic.remove(Constants.TOPIC_TWEETS_CURR_FETCHED_LIST);
		topic.put(Constants.TOPIC_TWEETS, tweets);
		log.fatal("fetchTopicTweets,endTime:"+(new Date()).toString()+"current get tweetsNum:"+tweets.size());
		return datum;
	}
	
	/**
	 * move to next crawl instance
	 */
	@Override
	protected void moveNext() {

	}

	@Override
	public boolean isComplete(){
		return false;
	}
}
