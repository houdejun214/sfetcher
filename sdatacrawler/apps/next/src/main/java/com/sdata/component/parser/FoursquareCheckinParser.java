package com.sdata.component.parser;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;
import net.sf.json.JsonConfig;

import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;

public class FoursquareCheckinParser extends SdataParser {
	
	public static final Logger log = LoggerFactory.getLogger("SdataCrawler.FoursquareCheckinParser");

	//private static Pattern option = Pattern.compile("options\\['[a-zA-Z0-9]*'\\]")
	
	public FoursquareCheckinParser(Configuration conf, RunState state) {
		this.setConf(conf);
		this.setRunState(state);
	}
	
	@Override
	public ParseResult parseList(RawContent c) {
		Object currentState = c.getMetadata("currentState");
		JSONObject response = JSONObject.fromObject(c.getContent());
		ParseResult result = new ParseResult();
		JSONArray results = (JSONArray)response.remove("results");
		result.setMetadata(response);
		if(results!=null){
			for(Object obj:results){
				JSONObject t = (JSONObject)obj;
				FetchDatum datum = new FetchDatum();
				datum.addMetadata("twitterid", t.get("id"));
				datum.addMetadata("twitteruid", t.get("from_user_id"));
				datum.addMetadata("twittername", t.get("from_user_name"));
				datum.addMetadata("isCheckIn", false);
				datum.addMetadata("text", t.get("text"));
				datum.setCurrent(StringUtils.valueOf(currentState));
				result.addFetchDatum(datum);
			}
		}
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		if(StringUtils.isEmpty(c.getContent())){
			return null;
		}
		Document doc = this.parseHtmlDocument(c);
		Elements scripts = doc.select("script");
		JSONObject checkin = new JSONObject();
		for(Element script:scripts){
			if(script.hasAttr("src")){
				// this is a extra script file 
				continue;
			}
			String content = script.html();
			if(content.indexOf("fourSq.views.CheckinPage")>=0){
				Pattern optionPattern = Pattern.compile("checkin: *(\\{[^;]*\\}),[^\\}].*",Pattern.DOTALL);
				Matcher matcher = optionPattern.matcher(content);
				while(matcher.find()){
					String value = matcher.group(1).trim();
					JsonConfig jsonConfig = new JsonConfig();
					JSONObject obj = JSONObject.fromObject(value.toString(),jsonConfig);
					checkin.putAll(obj);
					break;
				}
				break;
			}
		}
		//
		Boolean isCheckIn = PatternUtils.find("/checkin/.*",c.getUrl());
		Element anchor = doc.select(".userName a").first();
		String userUrl="",userName="";
		if(anchor!=null){
			userUrl = anchor.absUrl("href");
			userName = anchor.text();
		}
		String userpic = this.selectLink(doc, ".leftCheckInHeader a img");
		String text = this.selectText(doc, ".newShout p");
		checkin.put("isCheckIn", isCheckIn);
		if(StringUtils.isNotEmpty(text)){
			checkin.put("text", text);
		}else{
			text =StringUtils.valueOf(c.getMetadata("text"));
			checkin.put("text", text);
		}
		checkin.put("userurl", userUrl);
		checkin.put("userpic", userpic);
		checkin.put("username", userName);
		checkin.put("checkinurl", c.getUrl());
		ParseResult result = new ParseResult();
		result.addAllMeta(checkin);
		return result;
	}
}
