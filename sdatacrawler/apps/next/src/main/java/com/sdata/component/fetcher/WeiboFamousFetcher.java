package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.org.json.JSONException;
import weibo4j.util.WeiboServer;

import com.sdata.component.parser.WeiboFamousParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.util.WebPageDownloader;

/**
 * Weibo star info fetcher implement
 * 
 * @author zhufb
 *
 */
public class WeiboFamousFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.WeiboStarFetcher");
	private static final String weiboStarUrl="http://data.weibo.com/top/influence/famous?class=0001&type=day";
	
	public WeiboFamousFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
	    String token = conf.get("token");
  		if(StringUtils.isEmpty(token)){
  			token = CrawlAppContext.state.getCrawlName();
  		}
		WeiboServer.init(token);
		
	    this.parser =  new WeiboFamousParser(conf,state); 
	}

	/**
	 * init stars（first 100） info 
	 * 
	 * @param weiboTopicParser
	 * @return
	 */
	private JSONArray initFamousList(){
		String content = WebPageDownloader.download(weiboStarUrl);
		return  ((WeiboFamousParser)parser).parseFamousList(content);
	}
	
	/* 
	 * fetch topic list info 
	 *
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatumList()
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		//init top topic current 
		JSONArray famousPersons = initFamousList();
		return parseJsonToDatum(famousPersons);
	}
	
	/* 
	 * fetch one topic detail contains description and tweet list under this topic
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatum(com.sdata.component.FetchDatum)
	 */
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		((WeiboFamousParser)parser).packageFamousInfo(datum);
		return datum;
	}

	/**
	 * Parse json data to datum 
	 * @param JSONObject json
	 * @return FetchDatum
	 */
	public List<FetchDatum> parseJsonToDatum(JSONArray jsons) {
		List<FetchDatum> list = new ArrayList<FetchDatum>();
		try {
			for(int i=0;i<jsons.size();i++){
				JSONObject json = (JSONObject)jsons.get(i);
				FetchDatum datum = new FetchDatum();
				datum.setUrl(json.getString(Constants.FAMOUS_HOMEPAGE));
				datum.setName(json.getString(Constants.FAMOUS_NAME));
				datum.setMetadata(json);
				list.add(datum);
			}
		} catch (JSONException e) {
			log.error("parseJsonToDatum error:" + e.getMessage());
			throw new RuntimeException("parseJsonToDatum error:" + e.getMessage(),e);
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
