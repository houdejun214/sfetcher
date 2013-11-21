package com.sdata.component.parser;


import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.WebPageDownloader;


public class GothereParser extends SdataParser {
	
	private static final String SHOW_URL = "http://gothere.sg/maps#q:";
	
	public static final Log log = LogFactory.getLog("SdataCrawler.AddressParser");

	public GothereParser(Configuration conf, RunState state) {
		setConf(conf);
		this.state = state;
	}

	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		Object email = null;
		Object markers = null;
		String content = c.getContent();
		if(!JSONUtils.isValidJSON(content)){
			log.warn("is not a valid json string!");
			return null;
		}
		JSONObject json = JSONObject.fromObject(content);
		JSONObject where = json.getJSONObject("where");
		if(where!=null && !where.isNullObject()){
			email = where.get("email");
			markers = where.get("markers");
		}else{
			return null;
		}
		if (email == null || markers==null)
			return null;
		JSONArray markerList = (JSONArray)markers;
		if(markerList.size()==1){
			result.addFetchDatum(newFetchDatum(c, (List)email, 0,markerList));
		}else{
			JSONArray emailList = (JSONArray)email;
			for(int i=0;i<markerList.size();i++){
				result.addFetchDatum(newFetchDatum(c, (List)emailList.get(i+1),i,markerList));
			}
		}
		return result;
	}

	private FetchDatum newFetchDatum(RawContent c, List info, int count, JSONArray markerList) {
		JSONObject mark = markerList.getJSONObject(count);
		FetchDatum result = new FetchDatum();
		Object postcode = c.getMetadata(Constants.ADDRESS_POSTCODE);
		String mongoId = postcode + "" + count;
		Map<String, Object> meta = new HashMap<String, Object>();
		meta.put(Constants.ADDRESS_NAME, info.get(0).toString());
		meta.put(Constants.ADDRESS, info.get(1).toString());
		meta.put(Constants.ADDRESS_POSTCODE,postcode);
		meta.put("latlng", mark.get("latlng"));
		meta.put("url", SHOW_URL+postcode);
		meta.put(Constants.OBJECT_ID, mongoId);
		result.setMetadata(meta);
		result.setUrl(c.getUrl());
		result.setName(mongoId);
		return result;
	}

	public String download(String url) {
		String content = null;
		while (true) {
			content = WebPageDownloader.download(url);
			if (StringUtils.isEmpty(content)) {
				log.warn("*********获取内容为空,正在等待,5分钟后继续访问...");
				return null;
			}
			if (content.contains("对不起，你访问的太快了")) {
				log.warn("*********访问频率太快，正在等待，10分钟后继续访问...");
				sleep(60 * 10);
				continue;
			}
			return content;
		}
	}

	private void sleep(int s) {
		try {
			Thread.sleep(s * 1000);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
