package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Status;
import twitter4j.Trends;

import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.component.parser.TwitterTopicParser;
import com.sdata.component.site.TopicAPI;
import com.sdata.component.site.TwitterApi;
import com.sdata.component.site.TwitterTopicApi;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;

public class TwitterTopicFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.TwitterTopicFetcher");
	
	private final TwitterTopicApi api = new TwitterTopicApi();
	
	private int tweetsCountInOneTopic;//wo should get tweet~s count number every topic
	
	private String[] topicLocation=null;
	
	private TopicAPI topicAPI;

	private final TwitterApi tapi = new TwitterApi();
	
	public TwitterTopicFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		tweetsCountInOneTopic = conf.getInt("tweetsCountInOneTopic",200);
		this.parser = new TwitterTopicParser(conf,state);
		topicLocation = conf.get("topicLocation").split(",");
		topicAPI = new TopicAPI(conf,state);
		String consumerKey = conf.get("ConsumerKey");
		String consumerSecret = conf.get("ConsumerSecret");
		String accessTokenName = conf.get("AccessToken");
		String accessTokenSecret = conf.get("AccessTokenSecret");
		tapi.setOAuth(consumerKey, consumerSecret, accessTokenName, accessTokenSecret);
	}

	@Override
	public List<FetchDatum> fetchDatumList() {
		List<FetchDatum> resultList = new ArrayList<FetchDatum>();
		if(topicLocation!=null && topicLocation.length>0){
			for(int i=0;i<topicLocation.length;i++){
				String location = topicLocation[i];//worldwide or singapore
				Trends trends = tapi.trends(Integer.valueOf(location));
				if(trends==null){
					log.error("Twitter.com visit abnormal *************************************");
				}else{
					ParseResult parseList = ((TwitterTopicParser)parser).parseList(trends);
					List<FetchDatum> fetchList = parseList.getFetchList();
					resultList.addAll(fetchList);
				}
			}
		}
		//deal topics
		resultList = topicAPI.parseTwitterTopic(resultList);
		log.info("fetch topics [{}]",resultList.size());
		return resultList;
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		Map<String,Object> meta = datum.getMetadata();
		String query = meta.get("query").toString();
		List<Map> list = new ArrayList<Map>();
		List<Status> result = tapi.search(query, this.tweetsCountInOneTopic);
		if(result==null || result.size() == 0){
			log.error("Twitter.com visit abnormal *************************************");
		}else{
			for(Status status:result){
				Map map = status.getJSONObject().toMap();
				list.add(map);
			}
			datum.addMetadata(Constants.TOPIC_TWEETS, list);
		}
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
	

	private String dateFormat(Date time){
		return DateTimeUtils.format(time, "yyyy-MM-dd HH:mm:ss");
	}
	
	private String getTimeSpan(Date time1,Date time2){
		return "["+DateTimeUtils.format(time1, "yyyy-MM-dd HH:mm:ss")+" - " + DateTimeUtils.format(time2, "yyyy-MM-dd HH:mm:ss")+ "]";
	}
	
	private Date parseToDate(String str){
		return DateTimeUtils.parse(str, "yyyy-MM-dd HH:mm:ss");
	}
}
