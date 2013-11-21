package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.Timeline;
import weibo4j.Weibo;
import weibo4j.model.WeiboException;
import weibo4j.util.WeiboServer;

import com.lakeside.core.utils.JSONUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;

/**
 * Weibo info fetcher implement
 * 
 * @author zhufb
 *
 */
public class WeiboFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.WeiboFetcher");
	private Timeline tm = null ;
	private int fetchNum;
	
	public WeiboFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		this.fetchNum  = this.getConfInt(Constants.WEIBO_FETCH_NUM, 200);
		//init token
		Weibo.initToken();
		String token = conf.get("token");
  		if(StringUtils.isEmpty(token)){
  			token = CrawlAppContext.state.getCrawlName();
  		}
		WeiboServer.init(token);
		tm = new Timeline();
	}

	@Override
	public List<FetchDatum> fetchDatumList() {

		List<FetchDatum> fetchList = new ArrayList<FetchDatum>();
		try {
			weibo4j.org.json.JSONObject json = tm.getPublicTimelineJSON(fetchNum, 0);
			fetchList = parseWeiboJsonToDatum(json.getJSONArray(Constants.WEIBO_STATUSES));
		} catch (WeiboException e) {
			log.error("fetchDatumList error:" + e.getMessage());
			throw new NegligibleException("fetchDatumList error:" + e.getMessage(),e);
		}
		log.info("fetch tweets list size:"+fetchList.size());
		return fetchList;
	}
	
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
	
	/**
	 * Parse json data to datum 
	 * @param JSONObject json
	 * @return FetchDatum
	 */
	public List<FetchDatum> parseWeiboJsonToDatum(weibo4j.org.json.JSONArray jsons) {
		List<FetchDatum> list = new ArrayList<FetchDatum>();
		for(int i=0;i<jsons.length();i++){
			weibo4j.org.json.JSONObject json = (weibo4j.org.json.JSONObject)jsons.get(i);
			FetchDatum datum = new FetchDatum();
			try{
				JSONObject result = JSONUtils.map2JSONObj(json);
				String id = result.getString("id");
				result.put(Constants.OBJECT_ID, Long.parseLong(id));
				datum.setId(id);
				datum.setMetadata(result);
				list.add(datum);
			}catch(Exception e){
				log.error("parseWeiboJsonToDatum error:" + e.getMessage());
				continue;
			}
		}
		return list;
	}
	
}
