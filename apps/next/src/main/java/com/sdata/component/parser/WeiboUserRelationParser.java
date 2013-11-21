package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.component.site.WeiboUserAPI;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;


/**
 * @author zhufb
 *
 */
public class WeiboUserRelationParser extends SdataParser{
	
	TweetsMgDao dao = new TweetsMgDao();
	private static final Log log = LogFactory.getLog("SdataCrawler.WeiboUserRelationParser");
	private WeiboFamousParser weiboFamousParser;
	private WeiboUserAPI weiboUserFetcher;
	private UserMgDao userDao = new UserMgDao();;
	public WeiboUserRelationParser(Configuration conf,RunState state){
		setConf(conf);
		setRunState(state);
		weiboFamousParser = new WeiboFamousParser(conf, state);
		weiboUserFetcher = new WeiboUserAPI(conf, state);
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.userDao.initilize(host, port, dbName);
	}
	
	/**
	 * for save to sqlite
	 * @param content
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> parseUserRelationList(String content) {
		JSONArray array = weiboFamousParser.parseFamousList(content);
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		Iterator<Map<String,Object>> iterator = array.iterator();
		while(iterator.hasNext()){
			Map<String,Object> famous =  iterator.next();
			Map<String,Object> item = new HashMap<String,Object>();
			//save seed user
			String name = String.valueOf(famous.get(Constants.FAMOUS_NAME));
			JSONObject user = new JSONObject() ;
			try{
				user = weiboUserFetcher.fetchUserByName(name);
			}catch(Exception e){
				log.warn("fetch one seed error,name:"+name+"can't catch it's user info!");
				e.printStackTrace();
				continue;
			}
			Long uid = user.getLong(Constants.USER_ID);
			user.put(Constants.OBJECT_ID, uid);
			//parse queue
			item.put(Constants.QUEUE_KEY,uid);
			item.put(Constants.QUEUE_NAME,name);
			item.put(Constants.QUEUE_URL,famous.get(Constants.FAMOUS_HOMEPAGE));
			item.put(Constants.QUEUE_DEPTH,Constants.QUEUE_DEPTH_ROOT);
			item.put(Constants.QUEUE_DISPOSE,Constants.FLAG_NO);
			item.put(Constants.USER, user);
			result.add(item);
		}
		return result;
	}
	
	/**
	 * for save to sqlite
	 * 
	 * @param list
	 * @param depth
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List<Map<String, Object>> parseUserRelationList(String uid,int depth) {
		JSONArray friends = weiboUserFetcher.fetchFriendsById(uid,200);
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		Iterator<JSONObject> iterator = friends.iterator();
		while(iterator.hasNext()){
			JSONObject friend = iterator.next();
			result.add(this.parseFriends(friend, depth));
		} 
		JSONArray follows = weiboUserFetcher.fetchFollowsById(uid,5);
		iterator = follows.iterator();
		while(iterator.hasNext()){
			JSONObject follow = iterator.next();
			result.add(this.parseFollows(follow, depth));
		} 
		return result;
	}
	
	/**
	 * get user info 
	 * 
	 * @param uid
	 * @param depth
	 * @return
	 */
	public Map<String, Object> parseFriends(JSONObject user,int depth) {
		Map<String,Object> item = new HashMap<String,Object>();
		user.put(Constants.OBJECT_ID, user.get(Constants.USER_ID));
		item.put(Constants.QUEUE_KEY,user.get(Constants.USER_ID));
		item.put(Constants.QUEUE_DEPTH,depth);
		item.put(Constants.QUEUE_DISPOSE,Constants.FLAG_NO);
		item.put(Constants.USER_RELATION_FRIENDS, user);
		item.put(Constants.QUEUE_NAME,user.get(Constants.USER_NAME));
		item.put(Constants.QUEUE_URL,user.get(Constants.USER_HOMEPAGE));
		return item;
	}

	/**
	 * get user info 
	 * 
	 * @param uid
	 * @param depth
	 * @return
	 */
	public Map<String, Object> parseFollows(JSONObject user,int depth) {
		Map<String,Object> item = new HashMap<String,Object>();
		user.put(Constants.OBJECT_ID, user.get(Constants.USER_ID));
		item.put(Constants.QUEUE_KEY,user.get(Constants.USER_ID));
		item.put(Constants.QUEUE_DEPTH,depth);
		item.put(Constants.QUEUE_DISPOSE,Constants.FLAG_NO);
		item.put(Constants.USER_RELATION_FOLLOWS, user);
		item.put(Constants.QUEUE_NAME,user.get(Constants.USER_NAME));
		item.put(Constants.QUEUE_URL,user.get(Constants.USER_HOMEPAGE));
		return item;
	}
	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		return result;
	}

}
