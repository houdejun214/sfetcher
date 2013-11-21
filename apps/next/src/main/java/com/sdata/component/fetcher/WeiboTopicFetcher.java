package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.org.json.JSONException;
import weibo4j.util.WeiboServer;

import com.sdata.component.parser.WeiboTopicParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.util.WebPageDownloader;

/**
 * Weibo topic info fetcher implement
 * 
 * @author zhufb
 *
 */
public class WeiboTopicFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.WeiboTopicFetcher");
	private static final String weiboTopicUrl="http://news.sina.com.cn/iframe/tblog/js/hottopic/jsondata_new.js";
	
	public WeiboTopicFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
	    this.parser =  new WeiboTopicParser(conf,state);
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
		String content = WebPageDownloader.download(weiboTopicUrl);
		return  ((WeiboTopicParser)parser).parseTopicList(content);
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
		return parseJsonToDatum(topicList);
	}
	
	/* 
	 * fetch one topic detail contains description and tweet list under this topic
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatum(com.sdata.component.FetchDatum)
	 */
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		((WeiboTopicParser)parser).packageTopicTweetsList(datum);
		return datum;
	}

	/**
	 * Parse json data to datum 
	 * @param JSONObject json
	 * @return FetchDatum
	 */
	public List<FetchDatum> parseJsonToDatum(List jsons) {
		List<FetchDatum> list = new ArrayList<FetchDatum>();
		try {
			if(jsons == null){
				log.error("parseJsonToDatum warning: json is null!");
				throw new NegligibleException("parseJsonToDatum warning: json is null!");
			}
			for(int i=0;i<jsons.size();i++){
				Map json = (Map)jsons.get(i);
				FetchDatum datum = new FetchDatum();
				datum.setId(json.get(Constants.TOPIC_ID).toString());
				datum.setUrl(json.get(Constants.TOPIC_URL).toString());
				datum.setMetadata(json);
				list.add(datum);
			}
		} catch (Exception e) {
			log.error("parseJsonToDatum error:" + e.getMessage());
			throw new NegligibleException("parseJsonToDatum warning:" + e.getMessage(),e);
		}
		return list;
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
