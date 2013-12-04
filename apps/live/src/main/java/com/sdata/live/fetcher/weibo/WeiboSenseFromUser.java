package com.sdata.live.fetcher.weibo;

import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONException;
import net.sf.json.JSONObject;
import weibo4j.Weibo;
import weibo4j.model.Paging;
import weibo4j.model.PostParameter;
import weibo4j.model.WeiboException;
import weibo4j.util.WeiboConfig;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.FetchDatum;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

public class WeiboSenseFromUser extends WeiboSenseFrom {
	private int TIMES = 3;
	public WeiboSenseFromUser(){
		
	}
	
	public List<FetchDatum> getData(SenseCrawlItem item) {
		String from = StringUtils.valueOf(item.getParam(CrawlItemEnum.ACCOUNT.getName()));
		if(StringUtils.isEmpty(from)){
			return null;
		}
		JSONArray tweets = fetchTweetsBySname(from);
		return this.parseJsonToDatum(tweets, item);
	}
	
	
	/**
	 * 因为是取最新的数据 所以每次只取 第一页 200条，不用翻页取
	 * 
	 * fetch user's tweets by user id
	 * 
	 * @param userId
	 * @return
	 */
	public JSONArray fetchTweetsBySname(String sname) {
		List<Object> tweets = new ArrayList<Object>();
		Paging paging = new Paging();
		int page=1;
		int time = 0;
		while(true){
			try {
				paging.setPage(page);
				weibo4j.org.json.JSONObject jsonStatus = Weibo.client.get(WeiboConfig.getValue("baseURL")+ "statuses/user_timeline.json",
								new PostParameter[] {
								new PostParameter("screen_name", sname),
								new PostParameter("since_id", 0),
								new PostParameter("count", 200),
								new PostParameter("base_app", 0),
								new PostParameter("feature", 0) },paging).asJSONObject();
				List<Object> jsonArray=null;
				if(!jsonStatus.isNull("statuses")){				
					jsonArray = jsonStatus.getJSONArray("statuses");
					tweets.addAll(jsonArray);
				}
				return JSONUtils.list2JSONArray(tweets);
		  }catch (WeiboException e) {
				if(time++>TIMES){
					throw new NegligibleException("fetchTweetsByUserId  error times >:"+TIMES + e.getMessage(),e);
				}
				sleep(60*10);
		  }
		}
	}
		
	
	/**
	 * Parse json data to datum 
	 * @param JSONObject json
	 * @return FetchDatum
	 */
	private List<FetchDatum> parseJsonToDatum(JSONArray jsons,SenseCrawlItem item) {
		List<FetchDatum> list = new ArrayList<FetchDatum>();
		try {
			for(int i=0;i<jsons.size();i++){
				JSONObject matadata = (JSONObject)jsons.get(i);
				SenseFetchDatum datum = new SenseFetchDatum();
				String turl = getTweetUrl(matadata);
				matadata.put("url", turl);
				datum.setUrl(turl);
				datum.setId(String.valueOf(matadata.get("id")));
				datum.setMetadata(matadata);
				datum.setCrawlItem(item);
				list.add(datum);
			}
		} catch (JSONException e) {
			throw new RuntimeException("parseJsonToDatum error:" + e.getMessage(),e);
		}
		return list;
	}

	@Override
	public SenseFetchDatum getDatum(SenseFetchDatum datum) {
		return datum;
	}
	
}
