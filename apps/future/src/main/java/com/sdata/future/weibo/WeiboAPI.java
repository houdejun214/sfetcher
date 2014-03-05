package com.sdata.future.weibo;

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

/**
 * Weibo api
 * 
 * @author zhufb
 *
 */
public class WeiboAPI {
	
	private static final Logger log = LoggerFactory.getLogger("WeiboAPI");
	private Timeline tm = new Timeline();
	private static Object syn = new Object();
	private static WeiboAPI api;
	
	private WeiboAPI() {
		//init token
		Weibo.initToken();
	}
	
	public static WeiboAPI getInstance() {
		if(api ==null){
			synchronized(syn){
				if(api ==null){
					api = new WeiboAPI();
				}
			}
		}
		return api;
	}

	/**
	 * fetch one tweet by id
	 * 
	 * @param id
	 * @return
	 */
	public Object fetchUserId(String name) {
		try {
			weibo4j.org.json.JSONObject jsonStatus = Weibo.client.get(WeiboConfig.getValue("baseURL")+ "users/show.json",
							new PostParameter[] {
							new PostParameter("screen_name", name),
							new PostParameter("source", 0)}).asJSONObject();
			return jsonStatus.get("id");
	  }catch (WeiboException e) {
	  }
		return null;
	}
	/**
	 * fetch one tweet by id
	 * 
	 * @param id
	 * @return
	 */
	public JSONObject fetchOneTweet(String id) {
		weibo4j.org.json.JSONObject json = null;
		try {
			json = tm.showStatusJson(id);
			return JSONUtils.map2JSONObj(json);
		} catch (WeiboException e) {
			log.error("fetchOneTweet error:" + e.getMessage());
		}
		return null;
	}
	
	/**
	 * fetch user's tweets by user id
	 * 
	 * @param userId
	 * @return
	 */
	public JSONArray fetchTweetsByUserId(String userId,Long sinceId,int count) {
		List<Object> tweets = new ArrayList<Object>();
		Paging paging = new Paging();
		try {
			paging.setPage(1);
			weibo4j.org.json.JSONObject jsonStatus = Weibo.client.get(WeiboConfig.getValue("baseURL")+ "statuses/user_timeline.json",
							new PostParameter[] {
							new PostParameter("uid", userId),
							new PostParameter("since_id", sinceId.toString()),
							new PostParameter("count", count),
							new PostParameter("base_app", 0),
							new PostParameter("source", 0),new PostParameter("feature", 0) },paging).asJSONObject();
			if(!jsonStatus.isNull("statuses")){				
				tweets = jsonStatus.getJSONArray("statuses");
			}
	  }catch (WeiboException e) {
	  }
	  return JSONUtils.list2JSONArray(tweets);
	}
	
	/**
	 * fetch user's tweets by user name
	 * 
	 * @param userName
	 * @return
	 */
	public JSONArray fetchTweetsByUserName(String userName,Long sinceId,int count) {
		List<Object> tweets = new ArrayList<Object>();
		Paging paging = new Paging();
		try {
			paging.setPage(1);
			weibo4j.org.json.JSONObject jsonStatus = Weibo.client.get(WeiboConfig.getValue("baseURL")+ "statuses/user_timeline.json",
							new PostParameter[] {
							new PostParameter("screen_name", userName),
							new PostParameter("since_id", sinceId.toString()),
							new PostParameter("count", count),
							new PostParameter("base_app", 0),
							new PostParameter("source", 0),new PostParameter("feature", 0) },paging).asJSONObject();
			if(!jsonStatus.isNull("statuses")){				
				tweets = jsonStatus.getJSONArray("statuses");
			}
	  }catch (WeiboException e) {
	  }
	  return JSONUtils.list2JSONArray(tweets);
	}
}
