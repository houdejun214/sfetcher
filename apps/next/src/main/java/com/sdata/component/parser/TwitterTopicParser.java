package com.sdata.component.parser;


import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import twitter4j.Location;
import twitter4j.Trend;
import twitter4j.Trends;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.parser.html.util.DocumentUtils;
import com.sdata.core.util.JsoupUtils;


public class TwitterTopicParser extends SdataParser{
	
	public static final Log log = LogFactory.getLog("SdataCrawler.TwitterTopicParser");

	private static final String tweetsInTrendsRequestUrl="https://twitter.com/i/search/timeline?type=recent&src=tren&include_available_features=1&include_entities=1&max_id=%s&q=%s";
	private int tweetsCountInOneTopic;
	public TwitterTopicParser(Configuration conf,RunState state){
		setConf(conf);
		tweetsCountInOneTopic = super.getConfInt("tweetsCountInOneTopic", 200);
	}

//	public ParseResult parseList(RawContent c) {
//		ParseResult result = new ParseResult();
//		String content = c.getContent();
//		if(!content.startsWith("{")){
//			return result;
//		}
//		JSONObject JSObj = JSONObject.fromObject(content);
//		if(JSObj == null){
//			return null;
//		}
//		String html = JSObj.getString("module_html");
//		Map<String, Object> location = getLocationObj(JSObj);
//		Document document = DocumentUtils.parseDocument(html, c.getUrl());
//		Elements els = document.select(".trend-items li.trend-item");
//		Iterator<Element> iterator = els.iterator();
//		while(iterator.hasNext()){
//			FetchDatum datum = new FetchDatum();
//			Map<String,Object> trendObj = new HashMap<String, Object>();
//			Element el = iterator.next();
//			trendObj.put("name", el.text());
//			trendObj.put("url", JsoupUtils.getLink(el, "a"));
//			trendObj.put("location", location);
//			datum.setMetadata(trendObj);
//			result.addFetchDatum(datum);
//		}
//		return result;
//	}
//	
//	private Map<String,Object> getLocationObj(JSONObject JSObj){
//		Map<String,Object> locObj = new HashMap<String, Object>();
//		String locname = "Singapore";
//		Integer woeid = JSObj.getInt("woeid");
//		if(woeid!=null&&woeid == 1){
//			locname = "Worldwide";
//		}
//		locObj.put("woeid", woeid);
//		locObj.put("locname", locname);
//		return locObj;
//	}
	
	public ParseResult parseList(Trends trends) {
		ParseResult result = new ParseResult();
		Location location = trends.getLocation();
		JSONObject locationObj = new JSONObject();
		locationObj.put("woeid", location.getWoeid());
		locationObj.put("locname", location.getName());
		Date created_at =trends.getTrendAt();
		Date as_of =trends.getAsOf();
		for(Trend t:trends.getTrends()){
			JSONObject trendObj = new JSONObject();
			FetchDatum datum = new FetchDatum();
			trendObj.put("created_at", created_at);
			trendObj.put("as_of", as_of);
			trendObj.put("location", locationObj);
			trendObj.put("url",t.getURL());
			trendObj.put("query",t.getQuery());
			trendObj.put("name", t.getName());
			datum.setMetadata(trendObj);
			result.addFetchDatum(datum);
		}
		return result;
	}

//	public ParseResult parseSingle(RawContent c) {
//		ParseResult result = new ParseResult();
//		JSONArray tweetsList = new JSONArray();
//		String content = c.getContent();
//		if(!content.startsWith("{")){
//			return null;
//		}
//		JSONObject JSObj = JSONObject.fromObject(content);
//		if(JSObj == null){
//			return null;
//		}
//		String served_by_blender = StringUtils.valueOf(JSObj.get("served_by_blender")) ;
//		String next_page = StringUtils.valueOf(JSObj.get("next_page")) ;
//		String error = StringUtils.valueOf(JSObj.get("error")) ;
//		JSONArray statuses = (JSONArray)JSObj.get("statuses");
//		Iterator<JSONObject> statusesIterator= statuses.iterator();
//		while(statusesIterator.hasNext()){
//			JSONObject statusObj = statusesIterator.next();
//			statusObj.put("served_by_blender", served_by_blender);
//			statusObj.put("next_page", next_page);
//			statusObj.put("error", error);
//			tweetsList.add(statusObj);
//		}
//		Map<String, JSONArray> metadata = new HashMap<String,JSONArray>();
//		metadata.put(Constants.TOPIC_TWEETS, tweetsList);
//		result.setMetadata(metadata);
//		return result;
//	}
	
	

}
