package com.sdata.sense.parser.tencentHot;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.fetcher.tencent.TencentJsonParser;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class TencentHotJsonParser extends TencentJsonParser {

	/* (non-Javadoc)
	 * @see com.sdata.sense.fetcher.tencent.TencentJsonParser#parseTencentAPITweets(com.sdata.sense.item.SenseCrawlItem, com.sdata.core.RawContent)
	 */
	public ParseResult parseTencentAPITweets(SenseCrawlItem item,RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		JSONObject JSONObj = JSONObject.fromObject(content);
		if(JSONObj == null){
			JSONObj = new JSONObject();
			JSONObj.put("hasnext", "1");
			result.setMetadata(JSONObj);
			return result;
		}
		Object o = JSONObj.get("data");
		if(o== null||!(o instanceof JSONObject)){
			JSONObj.put("hasnext", "1");
			result.setMetadata(JSONObj);
			return result;
		}
		JSONObject data = (JSONObject)o;
		String hasnext = StringUtils.valueOf(data.get("hasnext"));
		JSONObj.put("hasnext", hasnext);
		String pos = StringUtils.valueOf(data.get("pos"));
		JSONObj.put("pos", pos);
		Object info = data.get("info");
		if(info==null||!(info instanceof JSONArray)){
			JSONObj.put("hasnext", "1");
			result.setMetadata(JSONObj);
			return result;
		}
		String lastid = null;
		String pagetime=null;
		Iterator<JSONObject> infoIterator= ((JSONArray)info).iterator();
		while(infoIterator.hasNext()){
			Map<String,Object> tweetObj = new HashMap<String, Object>();
			tweetObj.putAll(infoIterator.next());
			if(lastid == null){
				lastid = StringUtils.valueOf(tweetObj.get("id"));
				pagetime = StringUtils.valueOf(tweetObj.get("timestamp"));
			}
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
		JSONObj.put("lastid", lastid);
		JSONObj.put("pagetime", pagetime);
		result.setMetadata(JSONObj);
		return result;
	}
}
