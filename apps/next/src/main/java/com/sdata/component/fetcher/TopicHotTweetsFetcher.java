package com.sdata.component.fetcher;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.util.WeiboServer;

import com.sdata.component.parser.TopicHotTweetsParser;
import com.sdata.core.Configuration;
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
public class TopicHotTweetsFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.WeiboTopicFetcher");
	private static final String weiboTopicUrl="http://news.sina.com.cn/iframe/tblog/js/hottopic/jsondata_new.js";
	
	public TopicHotTweetsFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
	    this.parser =  new TopicHotTweetsParser(conf,state);
	    String token = conf.get("token");
  		if(StringUtils.isEmpty(token)){
  			token = CrawlAppContext.state.getCrawlName();
  		}
		WeiboServer.init(token);
	}

	/**
	 * init topic list info 
	 * 
	 * @param weiboTopicParser
	 * @return
	 */
	private List initTopicList(){
		return ((TopicHotTweetsParser)parser).initFtlTopicList();
	}
	
	/* 
	 * fetch topic list info 
	 *
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatumList()
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		//init top topic current 
		List topicList = initTopicList();
		return topicList;
	}
	
	/* 
	 * fetch one topic detail contains description and tweet list under this topic
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatum(com.sdata.component.FetchDatum)
	 */
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
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
