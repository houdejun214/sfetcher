package com.sdata.component.site;

import java.util.ArrayList;
import java.util.List;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.Timeline;
import weibo4j.Weibo;
import weibo4j.model.Paging;
import weibo4j.model.PostParameter;
import weibo4j.model.WeiboException;
import weibo4j.util.WeiboConfig;

import com.lakeside.core.utils.JSONUtils;
import com.sdata.core.Configuration;
import com.sdata.core.NegligibleException;
import com.sdata.core.RunState;

/**
 * Weibo topic tweets info fetcher implement
 * 
 * @author zhufb
 *
 */
public class WeiboTweetAPI {
	
	private static final Logger log = LoggerFactory.getLogger("WeiboTweetAPI");
	private Timeline tm = new Timeline();
	private int TIMES = 3;
	
	public WeiboTweetAPI(Configuration conf,RunState state) {
		//init token
		Weibo.initToken();
	}

	/**
	 * fetch one tweet by id
	 * 
	 * @param id
	 * @return
	 */
	public JSONObject fetchOneTweet(String id) {
		weibo4j.org.json.JSONObject json = null;
		int time = 0;
		while(time<TIMES){
			try {
				json = tm.showStatusJson(id);
				return JSONUtils.map2JSONObj(json);
			} catch (WeiboException e) {
				log.error("fetchOneTweet error:" + e.getMessage());
				time++;
				sleep(60*10);
				continue;
			}
		}
		return null;
	}
	
	
	/**
	 * fetch user's tweets by user id
	 * 
	 * @param userId
	 * @return
	 */
	
	public JSONArray fetchTweetsByUserId(String userId,Long sinceId) {
		List<Object> tweets = new ArrayList<Object>();
		Paging paging = new Paging();
		int page = 1;
		while(true){
			try {
				paging.setPage(page);
				weibo4j.org.json.JSONObject jsonStatus = Weibo.client.get(WeiboConfig.getValue("baseURL")+ "statuses/user_timeline.json",
								new PostParameter[] {
								new PostParameter("uid", userId),
								new PostParameter("since_id", sinceId.toString()),
								new PostParameter("count", 100),
								new PostParameter("base_app", 0),
								new PostParameter("source", 0),new PostParameter("feature", 0) },paging).asJSONObject();
				List<Object> jsonArray=null;
				if(!jsonStatus.isNull("statuses")){				
					jsonArray = jsonStatus.getJSONArray("statuses");
					tweets.addAll(jsonArray);
				}
				if(jsonArray==null || jsonArray.size()==0){
					break;
				}
				page++;
		  }catch (WeiboException e) {
//			 e.printStackTrace();
			 break;
		  }
		}
		return JSONUtils.list2JSONArray(tweets);
	}
	
	/**
	 * fetch user's tweets by user name
	 * 
	 * @param userName
	 * @return
	 */
	public JSONArray fetchTweetsByUserName(String userName) {
		
		List<Object> tweets = new ArrayList<Object>();
		int time = 0;
		try {
			Paging paging = new Paging();
			int page=1;
			while(true){
				paging.setPage(page);
				weibo4j.org.json.JSONObject jsonStatus = Weibo.client.get(WeiboConfig.getValue("baseURL")+ "statuses/user_timeline.json",
								new PostParameter[] {
								new PostParameter("screen_name", userName),
								new PostParameter("base_app", 0),
								new PostParameter("feature", 0) },paging).asJSONObject();
				List<Object> jsonArray=null;
				if(!jsonStatus.isNull("statuses")){				
					jsonArray = jsonStatus.getJSONArray("statuses");
					tweets.addAll(jsonArray);
				}
				if(jsonArray==null || jsonArray.size()==0){
					break;
				}
				page++;
			}
		} catch (WeiboException e) {
			log.error("fetchTweetsByUserName error:" + e.getMessage());
			if(time++>TIMES){
				throw new NegligibleException("fetchTweetsByUserName  error times >:"+TIMES + e.getMessage(),e);
			}
			sleep(60*10);
		}
		return JSONUtils.list2JSONArray(tweets);
	}
	
	private void sleep(int s){
		try {
			Thread.sleep(s);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
}
