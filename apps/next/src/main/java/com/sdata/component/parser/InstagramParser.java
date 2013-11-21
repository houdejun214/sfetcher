package com.sdata.component.parser;


import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;

public class InstagramParser extends SdataParser {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.InstagramParser");
	
	public InstagramParser(Configuration conf,RunState state){
		setConf(conf);
	}

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		if(c.isEmpty()){
			log.warn("fetch content is empty!");
			return null;
		}
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		String status = String.valueOf(((JSONObject)JSObj.get("meta")).get("code"));
		if(StringUtils.isEmpty(status) || !status.equals("200")){
			return result;
		}
		HashMap<String, Object> pagination = (HashMap<String, Object>)JSObj.get("pagination");
		if(null!=pagination){
			result.setNextUrl(String.valueOf(pagination.get("next_url")));
		}
		JSONArray mediaData = (JSONArray)JSObj.get("data");
		if(mediaData!=null){
			Iterator<JSONObject> photoIterator= mediaData.iterator();
			while(photoIterator.hasNext()){
				JSONObject photoObj = photoIterator.next();
				FetchDatum datum = new FetchDatum();
				datum.setMetadata(photoObj);
				result.addFetchDatum(datum);
			}
		}
		return result;
	}
	
	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		String status = String.valueOf(((HashMap<String, Object>)JSObj.get("meta")).get("code"));
		if(StringUtils.isEmpty(status) || !status.equals("200")){
			return result;
		}
		Object jObj = JSObj.get("data");
		Map<String, Object> data = null;
		if(jObj instanceof JSONArray){
			if(c.getMetadata("type").equals(Constants.COMMENTS)){
				data = new HashMap<String, Object>();
				data.put(Constants.COMMENTS, jObj);
			}else if(c.getMetadata("type").equals(Constants.LIKES)){
				data = new HashMap<String, Object>();
				data.put(Constants.LIKES, jObj);
			}
		}else if(jObj instanceof Map){
			data = (Map<String, Object>)JSObj.get("data");
		}else{
			
		}
		result.setMetadata(data);
		return result;
	}
}
