package com.sdata.component.parser;


import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONArray;
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


public class TencentTweetsParser extends SdataParser{
	
	
	public static final Log log = LogFactory.getLog("SdataCrawler.TencentTweetsParser");
	
	public TencentTweetsParser(Configuration conf,RunState state){
		setConf(conf);
	}

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		JSONObject JSONObj = JSONObject.fromObject(content);
		JSONObject data = (JSONObject)JSONObj.get("data");
		if(data==null){
			result.setMetadata(JSONObj);
			return result;
		}
		String hasnext = StringUtils.valueOf(data.get("hasnext"));
		JSONObj.put("hasnext", hasnext);
		String pos = StringUtils.valueOf(data.get("pos"));
		JSONObj.put("pos", pos);
		JSONArray info = (JSONArray)data.get("info");
		Iterator<JSONObject> infoIterator= info.iterator();
		while(infoIterator.hasNext()){
//			JSONObject tweetObj = ;
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
				Date cdd = DateTimeUtils.getTimeFromUnixTime(crtdtL);
				tweetObj.put("crtdt",cdd );
			}
			String longitude = (String)tweetObj.get("longitude");
			String latitude = (String)tweetObj.get("latitude");
			if("0".equals(longitude) && "0".equals(latitude)){
				tweetObj.remove("longitude");
				tweetObj.remove("latitude");
			}
			Object object = tweetObj.get("image");
			if(object!=null&&object instanceof JSONArray){
				JSONArray imageArray = (JSONArray)object;
				if(imageArray!=null && imageArray.size()>0){
					for(int i=0;i<imageArray.size();i++){
						String imgUrl = (String)imageArray.get(i);
						imgUrl +="/460";
						imageArray.set(i, imgUrl);
					}
				}
			}
			Object object2 = tweetObj.get("source");
			if(object2!=null&&object2 instanceof JSONObject){
					Map<String,Object> retweetObj = new HashMap<String,Object>();
					retweetObj.putAll((JSONObject)object2);
					dealSource(retweetObj);
					tweetObj.remove("source");
					tweetObj.put(Constants.TWEET_RETWEETED, retweetObj);
			}
			datum.setMetadata(tweetObj);
			result.addFetchDatum(datum);
		}
		result.setMetadata(JSONObj);
		return result;
	}
	
	private void dealSource(Map<String,Object>  retweetObj) {
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
		JSONArray imageArray = (JSONArray)retweetObj.get("image");
		if(imageArray!=null && imageArray.size()>0){
			for(int i=0;i<imageArray.size();i++){
				String imgUrl = (String)imageArray.get(i);
				imgUrl +="/460";
				imageArray.set(i, imgUrl);
			}
		}
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
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
			String name = StringUtils.valueOf(userinfo.get("name")) ;
			userinfo.put(Constants.OBJECT_ID, name.hashCode());
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
