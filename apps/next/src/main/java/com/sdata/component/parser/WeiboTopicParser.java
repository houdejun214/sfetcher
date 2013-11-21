package com.sdata.component.parser;


import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.site.TopicAPI;
import com.sdata.component.site.WeiboTweetAPI;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.ApplicationContextHolder;
import com.sdata.core.util.WebPageDownloader;


/**
 * @author zhufb
 *
 */
public class WeiboTopicParser extends SdataParser{
	
	public static final String weiboHost="http://topic.weibo.com";
	public static final String topicUrl = "http://huati.weibo.com/aj_topic/list?all=1&pic=0&hasv=0&atten=0&prov=0&city=0&_t=0";
	TweetsMgDao dao = new TweetsMgDao();
	public static final Log log = LogFactory.getLog("SdataCrawler.WeiboTopicParser");
	WeiboTweetAPI weiboTweet;
	TopicAPI topicAPI;
	private static final Pattern longPattern = Pattern.compile("\\d+");
	
	public WeiboTopicParser(Configuration conf,RunState state) {
		setConf(conf);
		setRunState(state);
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		dao.initilize(host,port,dbName);
//		topicDao.initilize(host,port,dbName);
		weiboTweet = new WeiboTweetAPI(conf, state);
		topicAPI = new TopicAPI(conf,state);
	}
	
	/**
	 * fetch topic list from html content
	 * @param content
	 * @return
	 */
	@SuppressWarnings("unchecked")
	public List parseTopicList(String content) {
		if(StringUtils.isEmpty(content)){
			return null;
		}
		int b = content.indexOf("=");
		String jsonStr = content.substring(b+1);
		JSONObject jsonContent = null;
		List result = new ArrayList();
		try{
			jsonContent = JSONObject.fromObject(jsonStr);
			if(jsonContent == null){
				return null;
			}
			JSONArray topicList = jsonContent.getJSONArray("result");
			Iterator<JSONArray> array =topicList.iterator();
			List<String> idList = new ArrayList<String>();
			while(array.hasNext()){
				JSONArray jsonArray = array.next();
				Iterator<JSONObject> iterator = jsonArray.iterator();
				while(iterator.hasNext()){
					JSONObject json = iterator.next();
					
					//fetch topic id from link
					String url = json.getString("url");
					int begin = url.lastIndexOf("/");
					int end = url.lastIndexOf("?")==-1?url.length():url.lastIndexOf("?");
					String id = url.substring(begin+1,end);
						
					// topic repeat check 
					if(idList.contains(id)||!longPattern.matcher(id).matches()){
						continue;
					}
					idList.add(id);
					
					//package topic info
					Map topic = new HashMap();
					topic.put(Constants.TOPIC_ID, Long.parseLong(id));
					topic.put(Constants.TOPIC_CONTENT,json.get("topic"));
					topic.put(Constants.TOPIC_URL, url);
					topic.put(Constants.TOPIC_COUNT, json.get("keynum"));
					result.add(topic);
				}
			}
			//deal topics
			result = topicAPI.parse(result);
		}catch(Exception e){
			return null;
		}
		
		return result;
	}
	
	/**
	 * fetch topic's topic's description and tweets list  
	 * 
	 * @param datum
	 */
	@SuppressWarnings("unchecked")
	public void packageTopicTweetsList(FetchDatum datum) {
		//fetch topic
		//fetch topic first page
		if(Constants.TOPIC_STATE_END.equals(datum.getMetadata().get(Constants.TOPIC_STATE))){
			return;
		}
		
		Document doc = this.fetchPage(datum.getUrl().concat("?xsort=hot&pos=0&lable_t=tips#topic"));
		if(doc == null) {	
			return;
		}
		//fetch topic descriptions
		Element el = doc.select(".info_line_p,.topic_pictext").first();
		Elements dess = el.select("p");
		Iterator<Element> desIter = dess.iterator();
		StringBuffer description = new StringBuffer();
		while(desIter.hasNext()){
			Element des = desIter.next();
			description.append(des.html());
		}
		datum.getMetadata().put(Constants.TOPIC_DESCRIPTION, description.toString());
		Element epic = doc.select(".focus_pic img,.pic img").first();
		if(epic!=null){
			datum.getMetadata().put(Constants.TOPIC_IMG, epic.attr("src"));
		}
		// fetch weibo topic's tweets list
		JSONArray array = new JSONArray();
		JSONArray ftarray = new JSONArray();
		//fetch hot topics
	//	Elements pages = doc.select(".list div a:not(.current)");
		//if(pages == null||pages.size() == 0){
		this.packageNewTemplate(datum,doc);
		//}
		
	}
	// no use
	void packageOldTemplate(){

//		Collections.sort(pages,new ComparatorPages());
//		int fetchPages = super.getConfInt(Constants.WEIBO_FETCH_PAGE_NUM,0);
//		int length = pages.size() > fetchPages?fetchPages:pages.size();
//		for(int i=0;i<length&&doc!=null;i++){
//			Elements tweets = doc.select("dl[mid]");
//			Iterator<Element> iterator = tweets.iterator();
//			while(iterator.hasNext()){
//				Element tweet = iterator.next();
//				String id = tweet.attr("mid");
//				if(array.contains(id)){
//					continue;
//				}
//				array.add(id);
//				if(dao.isTweetExists(id)){
//					ftarray.add(id);
//				}
//			}
//			String url = weiboHost + pages.get(i).attr("href");
//			doc = this.fetchPage(url);
//		}
//		
//		//fetch all
//		doc = this.fetchPage(datum.getUrl());
//		pages = doc.select(".list div a:not(.current)");
//		Collections.sort(pages,new ComparatorPages());
//		fetchPages = super.getConfInt(Constants.WEIBO_FETCH_PAGE_NUM,0);
//		length = pages.size() > fetchPages?fetchPages:pages.size();
//		for(int i=0;i<length&&doc!=null;i++){
//			Elements tweets = doc.select("dl[mid]");
//			Iterator<Element> iterator = tweets.iterator();
//			while(iterator.hasNext()){
//				Element tweet = iterator.next();
//				String id = tweet.attr("mid");
//				if(array.contains(id)){
//					continue;
//				}
//				array.add(id);
//				if(dao.isTweetExists(id)){
//					ftarray.add(id);
//				}
//			}
//			String url = weiboHost + pages.get(i).attr("href");
//			doc = this.fetchPage(url);
//		}
//		datum.getMetadata().put(Constants.TOPIC_TWEETS, array);
//		datum.getMetadata().put(Constants.TOPIC_TWEETS_FETCHED_LIST, ftarray);
	}
	void packageNewTemplate(FetchDatum datum,Document doc){
		String keywords =  URLEncoder.encode(unescape(PatternUtils.getMatchPattern("\"keyword\":\"(.*?)\"", doc.toString(), 1)));
		doc = this.parseHtmlDocument(WeiboTopicPageDownloader.download(topicUrl.concat("&order=time&p=1&keyword=").concat(keywords)));
		int length = super.getConfInt(Constants.WEIBO_FETCH_PAGE_NUM,0);
		// fetch weibo topic's tweets list
		JSONArray array = new JSONArray();
		JSONArray ftarray = new JSONArray();
//		for(int i=0;i<length&&doc!=null;i++){
//			Elements tweets = doc.select(".list_feed_li");
//			Iterator<Element> iterator = tweets.iterator();
//			while(iterator.hasNext()){
//				Element tweet = iterator.next();
//				String id = tweet.attr("list-data").substring(4);
//				if(array.contains(id)){
//					continue;
//				}
//				array.add(id);
//				if(dao.isTweetExists(id)){
//					ftarray.add(id);
//				}
//			}
//			String	url = topicUrl.concat("&order=hot&p=").concat(String.valueOf(i+2)).concat("&keyword=").concat(keywords);
//			doc = this.parseHtmlDocument(SinaPageDownloader.download(url));
//		}
		
		//fetch all
		//doc = this.parseHtmlDocument(SinaPageDownloader.download(topicUrl.concat("&order=time&p=1&keyword=").concat(keywords)));
		length = super.getConfInt(Constants.WEIBO_FETCH_PAGE_NUM,0);
		for(int i=0;i<length&&doc!=null;i++){
			Elements tweets = doc.select(".list_feed_li");
			Iterator<Element> iterator = tweets.iterator();
			while(iterator.hasNext()){
				Element tweet = iterator.next();
				String id = tweet.attr("list-data").substring(4);
				String[] split = id.split("&");
				id = split[0];
				if(ftarray.contains(id)){
					continue;
				}
				JSONObject t = weiboTweet.fetchOneTweet(id);
				array.add(t);
				ftarray.add(id);
//				if(dao.isTweetExists(id)){
//				}else{
//					JSONObject fetchOneTweet = weiboTweet.fetchOneTweet(id);
//				}
			}
			String url = topicUrl.concat("&order=time&p=").concat(String.valueOf(i+2)).concat("&keyword=").concat(keywords);
			doc = this.parseHtmlDocument(WeiboTopicPageDownloader.download(url));
		}
		datum.getMetadata().put(Constants.TOPIC_TWEETS, array);
		datum.getMetadata().put(Constants.TOPIC_TWEETS_FETCHED_LIST, ftarray);
	}
	
	String unescape(String s) {
		int i = 0, len = s.length();
		char c;
		StringBuffer sb = new StringBuffer(len);
		while (i < len) {
			c = s.charAt(i++);
			if (c == '\\') {
				if (i < len) {
					c = s.charAt(i++);
					if (c == 'u') {
						c = (char) Integer.parseInt(s.substring(i, i + 4), 16);
						i += 4;
					} // add other cases here as desired...
				}
			} // fall through: \ escapes itself, quotes any character but u
			sb.append(c);
		}
		return sb.toString();
	}
	/**
	 * class pages comparator
	 * 
	 * @author zhufb
	 *
	 */
	@SuppressWarnings("rawtypes")
	class ComparatorPages implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			String href1 = ((Element)arg0).attr("href");
			String href2 = ((Element)arg1).attr("href");
			Integer page1 = Integer.valueOf(href1.substring(href1.lastIndexOf("=")+1));
			Integer page2 = Integer.valueOf(href2.substring(href2.lastIndexOf("=")+1));
			return page1.compareTo(page2);
		 }
	}

	/**
	 * 
	 * fetch next page
	 * 
	 * @param id
	 * @return
	 */
	private Document fetchPage(String url) {
		String content = WebPageDownloader.download(url);
		Document doc = parseHtmlDocument(content);
		return doc;
	}
	
	
	/**
	 * 
	 * fetch one single tweet
	 * 
	 * @param id
	 * @return
	 */
//	private JSONObject fetchTweet(String id) {
//		 return dao.isTweetExists(id)?null:weiboTweet.fetchOneTweet(id);
//	}

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
