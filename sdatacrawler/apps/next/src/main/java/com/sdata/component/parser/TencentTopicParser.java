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
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;


public class TencentTopicParser extends SdataParser{
	
	
	public static final Log log = LogFactory.getLog("SdataCrawler.TencentTopicParser");
	
	public TencentTopicParser(Configuration conf,RunState state){
		setConf(conf);
	}

	@Override
	public ParseResult parseList(RawContent c) {
		ParseResult result = new ParseResult();
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		JSONObject dataObj = (JSONObject)JSObj.get("data");
		String errcode = StringUtils.valueOf(JSObj.get("errcode"));
		String msg = StringUtils.valueOf(JSObj.get("msg"));
		String ret = StringUtils.valueOf(JSObj.get("ret"));
		Map<String,Object> prMetadata = new HashMap<String,Object>();
		prMetadata.put("errcode", errcode);
		prMetadata.put("msg", msg);
		prMetadata.put("ret", ret);
		result.setMetadata(prMetadata);
		
		JSONArray infoArr = (JSONArray)dataObj.get("info");
		Iterator<JSONObject> infoIterrator = infoArr.iterator();
		while(infoIterrator.hasNext()){
			FetchDatum datum = new FetchDatum();
			JSONObject infoObj = infoIterrator.next();
			Map<String,Object> map = new HashMap<String,Object>();
			map.putAll(infoObj);
			map.put(Constants.OBJECT_ID, UUIDUtils.getMd5UUID(StringUtils.valueOf(infoObj.get("id"))));
			datum.setMetadata(map);
			result.addFetchDatum(datum);
		}
		return result;
	}

	@Override
	public ParseResult parseSingle(RawContent c) {
		ParseResult result = new ParseResult();
		JSONArray tweetsList = new JSONArray();
		String content = c.getContent();
		JSONObject JSObj = JSONObject.fromObject(content);
		JSONObject dataObj = (JSONObject)JSObj.get("data");
		String errcode = StringUtils.valueOf(JSObj.get("errcode"));
		String msg = StringUtils.valueOf(JSObj.get("msg"));
		String ret = StringUtils.valueOf(JSObj.get("ret"));
		Map<String,Object> prMetadata = new HashMap<String,Object>();
		prMetadata.put("errcode", errcode);
		prMetadata.put("msg", msg);
		prMetadata.put("ret", ret);
		
		JSONArray infoArr = (JSONArray)dataObj.get("info");
		String id = StringUtils.valueOf(dataObj.get("id"));
		String hasnext = StringUtils.valueOf(dataObj.get("hasnext"));
		String isfav = StringUtils.valueOf(dataObj.get("isfav"));
		String totalnum = StringUtils.valueOf(dataObj.get("totalnum"));
		prMetadata.put("id", id);
		prMetadata.put("hasnext", hasnext);
		prMetadata.put("isfav", isfav);
		prMetadata.put("totalnum", totalnum);
		String lastTweetId ="0";
		String lastTime ="0";
		Iterator<JSONObject> infoIterator= infoArr.iterator();
		while(infoIterator.hasNext()){
			JSONObject infoObj = infoIterator.next();
			infoObj.put("id",String.valueOf(infoObj.get("id")));
			infoObj.put(Constants.OBJECT_ID,Long.valueOf(String.valueOf(infoObj.get("id"))) );
			tweetsList.add(infoObj);
			lastTweetId = StringUtils.valueOf(infoObj.get("id"));
			lastTime = StringUtils.valueOf(infoObj.get("timestamp"));
		}
		prMetadata.put("lastTweetId", lastTweetId);
		prMetadata.put("lastTime", lastTime);
		prMetadata.put(Constants.TOPIC_TWEETS, tweetsList);
		result.setMetadata(prMetadata);
		return result;
	}
	
	
	
	public Map<String,Object> parseTopicInfo(String content,String topicInfoUrl) {
		if(StringUtils.isEmpty(content)){
			return null;
		}
		Document doc = parseHtmlDocument(content);
		Map<String,Object> result = new HashMap<String,Object>();
		
		//fetch topic descriptions
		Element el = doc.select(".topic_txt_view").first();
		if(el!=null){
			result.put(Constants.TOPIC_DESCRIPTION, el.text());
			
			//fetch topic tweetsCount
			Element elTC = doc.select(".opt_hot .t").first();
			Long tweetsCount = 0l;
			if(elTC!=null){
				String tc = elTC.text();
				tc = tc.replace(",", "");
				String match = PatternUtils.getMatchPattern("广播(\\d+)", tc,1);
				if(!StringUtils.isEmpty(match)){
					tweetsCount = Long.valueOf(match.concat("0000"));
					result.put(Constants.TOPIC_COUNT, tweetsCount);
				}
			}
			return result;
		}
		
		//fetch topic descriptions
		el = doc.select(".con").first();
		if(el==null){
			log.info("parser content wrong,this page maybe donot have description info about topic["+topicInfoUrl+"]");
			return null;
		}
		Elements dess = el.select("p");
		Iterator<Element> desIter = dess.iterator();
		StringBuffer description = new StringBuffer();
		while(desIter.hasNext()){
			Element des = desIter.next();
			description.append(des.html());
		}
		result.put(Constants.TOPIC_DESCRIPTION, description.toString());
		
		//fetch topic image
		Element epic = doc.select(".userPic").first();
		Element pic = epic.select("img").first();
		if(pic!=null){
			result.put(Constants.TOPIC_IMG, pic.attr("src"));
		}
		
		//fetch topic tweetsCount
		Element elTC = doc.select(".tpubInfo .left").first();
		Elements bs = elTC.select("b");
		Long tweetsCount = 0l;
		String tc = bs.get(2).text();
		if(!StringUtils.isEmpty(tc)){
			tc =tc.replace(",", "");
			tc =tc.replace("万", "0000");
			tweetsCount = Long.valueOf(tc);
			result.put(Constants.TOPIC_COUNT, tweetsCount);
		}
		return result;
	}
	
	public Map<String,Object> parseTopicTweetsPage(String content) {
		if(StringUtils.isEmpty(content)){
			return null;
		}
		Document doc = parseHtmlDocument(content);
		Map<String,Object> result = new HashMap<String,Object>();
		List<String> tweetidsList = new ArrayList<String>();
		String nextPageUrl = "";
		
		//fetch tweetsid
		//fetch the top tweet
		
		Elements tweetsTop = doc.select("#talkListTop li");
		Iterator<Element> iteratorTop = tweetsTop.iterator();
		while(iteratorTop.hasNext()){
			Element tweet = iteratorTop.next();
			String tweetId = tweet.attr("id");
			if(StringUtils.isNotEmpty(tweetId)){
				tweetidsList.add(tweetId);
			}
		}
		
		//fetch other tweets
		Elements tweets = doc.select("#talkList li");
		Iterator<Element> iterator = tweets.iterator();
		while(iterator.hasNext()){
			Element tweet = iterator.next();
			String tweetId = tweet.attr("id");
			if(StringUtils.isNotEmpty(tweetId)){
				tweetidsList.add(tweetId);
			}
		}
		result.put("tweetidsList", tweetidsList);
		
		//fetch next page url
		Elements pageBtns = doc.select(".pageNav .pageBtn");
		Iterator<Element> pageIterator = pageBtns.iterator();
		while(pageIterator.hasNext()){
			Element pageEl = pageIterator.next();
			String text = pageEl.text();
			int index = text.indexOf(">>");
			if(index>0){
				nextPageUrl = pageEl.attr("href");
			}
		}
		result.put("nextPageUrl", nextPageUrl);
		return result;
	}
	
}
