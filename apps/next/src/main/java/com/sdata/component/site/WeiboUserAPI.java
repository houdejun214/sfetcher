package com.sdata.component.site;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.Friendships;
import weibo4j.Users;
import weibo4j.Weibo;
import weibo4j.model.WeiboException;

import com.lakeside.core.utils.JSONUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.NegligibleException;
import com.sdata.core.RunState;

/**
 * Weibo topic tweets info fetcher implement
 * 
 * @author zhufb
 *
 */
public class WeiboUserAPI {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.WeiboUserAPI");
	Users user = new Users();
	Friendships fship = new Friendships();
	private int TIMES = 3;
	public WeiboUserAPI(Configuration conf,RunState state) {
		//init token
		Weibo.initToken();
	}

	/**
	 * fetch one user by id
	 * 
	 * @param id
	 * @return
	 */
	public JSONObject fetchUserById(String uid) {
		weibo4j.org.json.JSONObject json = null;
		int time = 0;
		try {
			json = user.showUserJsonById(uid);
		} catch (WeiboException e) {
			log.error("fetchUserById error:" + e.getMessage());
			if(time++>TIMES){
				throw new NegligibleException("fetchUserById  error times >:"+TIMES + e.getMessage(),e);
			}
			sleep(60*10);
		}
		return JSONUtils.map2JSONObj(json);
	}
	
	/**
	 * fetch one user by name
	 * 
	 * @param name
	 * @return
	 */
	public JSONObject fetchUserByName(String name) {
		weibo4j.org.json.JSONObject json = null;
		int time = 0;
		while(time<TIMES){
			try {
				json = user.showUserJsonByScreenName(name);
				return JSONUtils.map2JSONObj(json);
			} catch (WeiboException e) {
				log.error("fetchUserByName error:" + e.getMessage());
				e.printStackTrace();
				time++;
				continue;
			}
		}
		return null;
	}
	
	/**
	 * fetch user's friends list
	 * 
	 * @param uid
	 * @return
	 */
	public JSONArray fetchFriendsById(String uid,int pagecount) {
		JSONArray result = new JSONArray();
		int cursor = 0;
		while(true){
			try{
				weibo4j.org.json.JSONObject friends = fship.getFriendsByID(uid,pagecount,cursor);
				JSONArray list2jsonArray = JSONUtils.list2JSONArray(friends.getJSONArray(Constants.USER_FRIENDS));
				result.addAll(list2jsonArray);
				if((cursor=friends.getInt("next_cursor"))-1<=0){
					break;
				}
			}catch(WeiboException e){
				log.error("fetchFriendsById error:" + e.getMessage());
//				e.printStackTrace();
				break;
			}
		}
		return result;
	}
	
	/**
	 * fetch user's follows list
	 * 
	 * @param uid
	 * @return
	 */
	public JSONArray fetchFollowsById(String uid,int pagecount) {
		JSONArray result = new JSONArray();
		int cursor = 0;
		int time = 0;
		while(true){
			try{
				weibo4j.org.json.JSONObject follows = fship.getFollowsByID(uid,pagecount,cursor);
				JSONArray list2jsonArray = JSONUtils.list2JSONArray(follows.getJSONArray(Constants.USER_FRIENDS));
				result.addAll(list2jsonArray);
				if((cursor=follows.getInt("next_cursor"))>=0){
					break;
				}
			}catch(WeiboException e){
				log.error("fetchFollowsById error:" + e.getMessage());
				if(time++>TIMES){
					throw new NegligibleException("fetchFollowsById  error times >:"+TIMES + e.getMessage(),e);
				}
				sleep(60*10);
			}
		}
		return result;
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
