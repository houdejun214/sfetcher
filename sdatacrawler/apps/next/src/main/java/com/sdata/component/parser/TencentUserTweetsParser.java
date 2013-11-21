package com.sdata.component.parser;


import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.json.JSONArray;
import net.sf.json.JSONNull;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;


public class TencentUserTweetsParser extends SdataParser{
	
	
	public static final Log log = LogFactory.getLog("SdataCrawler.TencentTweetsParser");
	
	public TencentUserTweetsParser(Configuration conf,RunState state){
		setConf(conf);
	}

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			return result;
		}
		JSONObject JSONObj = JSONObject.fromObject(content);
		if(JSONObj!=null){
			JSONObject data = (JSONObject)JSONObj.get("data");
			if(data==null){//have no tweets
				JSONObj.put("hasnext", "1");
				result.setMetadata(JSONObj);
				return result;
			}
			String hasnext = StringUtils.valueOf(data.get("hasnext"));
			JSONObj.put("hasnext", hasnext);
			JSONArray info = (JSONArray)data.get("info");
			if(info!=null){
				Iterator<JSONObject> infoIterator= info.iterator();
				while(infoIterator.hasNext()){
					Map<String,Object> tweetObj = new HashMap<String,Object>();
					tweetObj.putAll(infoIterator.next());
					FetchDatum datum = new FetchDatum();
					String idStr = StringUtils.valueOf(tweetObj.get("id"));
					datum.setId(idStr);
					tweetObj.put("id", Long.valueOf(idStr));
					tweetObj.put(Constants.OBJECT_ID, Long.valueOf(idStr));
					String crtdt = StringUtils.valueOf(tweetObj.get("timestamp"));
					if(StringUtils.isNotEmpty(crtdt)){
						Long crtdtL = Long.valueOf(crtdt);
						tweetObj.put("crtdt",DateTimeUtils.getTimeFromUnixTime(crtdtL));
					}
					String longitude = (String)tweetObj.get("longitude");
					String latitude = (String)tweetObj.get("latitude");
					if("0".equals(longitude) && "0".equals(latitude)){
						tweetObj.remove("longitude");
						tweetObj.remove("latitude");
					}
					Object img = tweetObj.get("image");
					if(img!=null && img instanceof  JSONArray){
						JSONArray imageArray = (JSONArray)img;
						for(int i=0;i<imageArray.size();i++){
							String imgUrl = (String)imageArray.get(i);
							imgUrl +="/460";
							imageArray.set(i, imgUrl);
						}
					}
					Object obj = tweetObj.get("source");
					if(obj!=null&&obj instanceof JSONObject){
						Map<String,Object> map = this.jsonToMap((JSONObject)obj);
//						JSONObject retweetObj = (JSONObject)obj;
						dealSource(map);
						tweetObj.remove("source");
						tweetObj.put(Constants.TWEET_RETWEETED, map);
					}
					datum.setMetadata(tweetObj);
					result.addFetchDatum(datum);
					JSONObj.put("lastid", idStr);
					JSONObj.put("pagetime", StringUtils.valueOf(tweetObj.get("timestamp")));
					
				}
			}else{
				JSONObj.put("hasnext", "1");
				log.info("content can't be parser :"+content);
			}
			result.setMetadata(JSONObj);
		}
		return result;
	}

	private void dealSource(Map<String,Object> retweetObj) {
		String idStr = StringUtils.valueOf(retweetObj.get("id"));
		retweetObj.put("id", Long.valueOf(idStr));
		retweetObj.put(Constants.OBJECT_ID, Long.valueOf(idStr));
		String crtdt = StringUtils.valueOf(retweetObj.get("timestamp"));
		if(StringUtils.isNotEmpty(crtdt)){
			Long crtdtL = Long.valueOf(crtdt);
			Date cdd = DateTimeUtils.getTimeFromUnixTime(crtdtL);
			retweetObj.put("crtdt",cdd );
		}
		String longitude = (String)retweetObj.get("longitude");
		String latitude = (String)retweetObj.get("latitude");
		if("0".equals(longitude) && "0".equals(latitude)){
			retweetObj.remove("longitude");
			retweetObj.remove("latitude");
		}
		Object object = retweetObj.get("image");
		if(object!=null &&  object instanceof JSONArray){
			JSONArray imageArray = (JSONArray)object;
			for(int i=0;i<imageArray.size();i++){
				String imgUrl = (String)imageArray.get(i);
				imgUrl +="/460";
				imageArray.set(i, imgUrl);
			}
		}
	}
	
	private Map<String,Object> jsonToMap(JSONObject jsonobj){
		Map<String,Object> map = new HashMap<String,Object>();
		Iterator<String> keys = jsonobj.keys();
		while( keys.hasNext()){
			String next = keys.next();
			Object object = jsonobj.get(next);
			if(object == null||object instanceof JSONNull){
				continue;
			}
			map.put(next, object);
		}
		return map;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			return result;
		}
		Map<String,Object> tweetObj = new HashMap<String,Object>();
		JSONObject JSONObj = JSONObject.fromObject(content);
		if(JSONObj!=null){
			tweetObj.put("errcode", JSONObj.get("errcode"));
			tweetObj.put("msg", JSONObj.get("msg"));
			tweetObj.put("ret", JSONObj.get("ret"));
			Object obj = JSONObj.get("data");
			if(obj!=null &&obj instanceof JSONObject){
				tweetObj = this.jsonToMap((JSONObject)obj);
				tweetObj.put("errcode", JSONObj.get("errcode"));
				tweetObj.put("msg", JSONObj.get("msg"));
				tweetObj.put("ret", JSONObj.get("ret"));
				String idStr = StringUtils.valueOf(tweetObj.get("id"));
				if(StringUtils.isEmpty(idStr)){
					tweetObj.put("hasdata", "0");
				}else{
					tweetObj.put("id", Long.valueOf(idStr));
					tweetObj.put(Constants.OBJECT_ID, Long.valueOf(idStr));
					String crtdt = StringUtils.valueOf(tweetObj.get("timestamp"));
					if(StringUtils.isNotEmpty(crtdt)){
						Long crtdtL = Long.valueOf(crtdt);
						Date cdd = DateTimeUtils.getTimeFromUnixTime(crtdtL);
						tweetObj.put("crtdt",cdd );
					}
					String longitude = (String)tweetObj.get("longitude");
					String latitude = (String)tweetObj.get("latitude");
					if("0".equals(longitude) && "0".equals(latitude)){
						tweetObj.remove("longitude");
						tweetObj.remove("latitude");
					}
					JSONArray imageArray = (JSONArray)tweetObj.get("image");
					if(imageArray!=null && imageArray.size()>0){
						for(int i=0;i<imageArray.size();i++){
							String imgUrl = (String)imageArray.get(i);
							imgUrl +="/460";
							imageArray.set(i, imgUrl);
						}
					}
					Object o = (Object)tweetObj.get("source");
					if(o!=null&&!(o instanceof JSONNull)){
						Map<String,Object> map = this.jsonToMap((JSONObject)o);
						dealSource(map);
						tweetObj.remove("source");
						tweetObj.put(Constants.TWEET_RETWEETED, map);
					}
					tweetObj.put("lastid", idStr);
					tweetObj.put("pagetime", StringUtils.valueOf(tweetObj.get("timestamp")));
				}
			}else{
				tweetObj.put("hasdata", "0");
			}
			result.setMetadata(tweetObj);
		}
		return result;
	}
	
}
