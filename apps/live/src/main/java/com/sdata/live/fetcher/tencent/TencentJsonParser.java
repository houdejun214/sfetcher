package com.sdata.live.fetcher.tencent;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.lakeside.core.utils.MapUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.live.IDBuilder;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class TencentJsonParser {
	
	private final String baseUrl = "http://t.qq.com/p/t/";
	private final String userBaseUrl = "http://t.qq.com/";
	
	public ParseResult parseTencentAPITweets(SenseCrawlItem item,RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		JSONObject JSONObj = JSONObject.fromObject(content);
		if(JSONObj == null){
			return result;
		}
		Object o = JSONObj.get("data");
		if(o==null||!(o instanceof JSONObject)){
			return result;
		}
		JSONObject data = (JSONObject)o;
		Object info = data.get("info");
		if(info==null||!(info instanceof JSONArray)){
			return result;
		}
		Iterator<JSONObject> infoIterator= ((JSONArray)info).iterator();
		while(infoIterator.hasNext()){
			Map<String,Object> tweetObj = new HashMap<String, Object>();
			tweetObj.putAll(infoIterator.next());
			Object object = tweetObj.get("source");
			if(object!=null&&object instanceof JSONObject ){
				JSONObject retweetObj = (JSONObject)object;
				String retIdStr = StringUtils.valueOf(retweetObj.get("id"));
				String retctStr = StringUtils.valueOf(retweetObj.get("count"));
				Long retId = Long.valueOf(retIdStr);
				FetchDatum retweetDatum = this.dealTweetsInfo(item,retweetObj);
				result.addFetchDatum(retweetDatum);
				tweetObj.remove("source");
				tweetObj.put(Constants.TWEET_RETWEETED_ID, retId);
				tweetObj.put(Constants.TWEET_SOURCE_RETWEETED_COUNT, Long.valueOf(retctStr));
			}
			FetchDatum datum = this.dealTweetsInfo(item,tweetObj);
			result.addFetchDatum(datum);
		}
		return result;
	}
	
	protected FetchDatum dealTweetsInfo(SenseCrawlItem item,Map<String,Object> tweetObj){
		SenseFetchDatum datum = new SenseFetchDatum();
		String idStr = StringUtils.valueOf(tweetObj.get("id"));
		datum.setId(idStr);
		tweetObj.put("id", Long.valueOf(idStr));
		tweetObj.put("url", baseUrl+idStr);
		tweetObj.put("uurl", userBaseUrl+tweetObj.get("name"));
		String head = StringUtils.valueOf(tweetObj.get("head"));
		if(!StringUtils.isEmpty(head)&&!head.endsWith("/180")){
			head = head.concat("/180");
		}
		tweetObj.put("head", head);
		String longitude = (String)tweetObj.get("longitude");
		String latitude = (String)tweetObj.get("latitude");
		if("0".equals(longitude) && "0".equals(latitude)){
			tweetObj.remove("longitude");
			tweetObj.remove("latitude");
		}
		Object object = tweetObj.get("image");
		if(object!=null&&object instanceof JSONArray){
			JSONArray imageArray = (JSONArray)object;
			for(int i=0;i<imageArray.size();i++){
				String imgUrl = (String)imageArray.get(i);
				imgUrl +="/460";
				imageArray.set(i, imgUrl);
			}
		}
		byte[] oid = IDBuilder.build(item, idStr);
		tweetObj = MapUtils.cleanMap(tweetObj);
		tweetObj.put(Constants.OBJECT_ID, oid);
		datum.setId(oid);
		datum.setCrawlItem(item);
		datum.setMetadata(tweetObj);
		return datum;
	}
	
	public ParseResult parseUserInfo(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		JSONObject userinfo = null;
		if(JSObj==null){
			userinfo = new JSONObject();
			userinfo.put("rerrcode", "1");
			userinfo.put("rmsg", "no user info");
			Map<String, JSONObject> metadata = new HashMap<String,JSONObject>();
			metadata.put(Constants.TENCENT_USER, userinfo);
			result.setMetadata(metadata);
			return result;
		}
		String errcode = StringUtils.valueOf(JSObj.get("errcode")) ;
		String msg = StringUtils.valueOf(JSObj.get("msg")) ;
		if(errcode.equals("0")){
			userinfo = (JSONObject)JSObj.get("data");
		}else{
			userinfo = new JSONObject();
		}
		userinfo.put("rerrcode", errcode);
		userinfo.put("rmsg", msg);
		Map<String, JSONObject> metadata = new HashMap<String,JSONObject>();
		metadata.put(Constants.TENCENT_USER, userinfo);
		result.setMetadata(metadata);
		return result;
	}
	
}
