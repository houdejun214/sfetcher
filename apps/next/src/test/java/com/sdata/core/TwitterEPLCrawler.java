package com.sdata.core;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONObject;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;

import twitter4j.TwitterException;

import com.lakeside.core.utils.JSONUtils;
import com.lakeside.core.utils.PatternUtils;
import com.lakeside.core.utils.time.DateFormat;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;
import com.sdata.component.site.TwitterApi;
import com.sdata.core.http.HttpPage;
import com.sdata.core.http.HttpPageLoader;

public class TwitterEPLCrawler {
	private static String URL = "https://twitter.com/i/search/timeline?type=recent&src=typd&include_available_features=1&include_entities=1&q=%s&max_id=%s";
	private static String ConsumerKey = "JxbZCJLzi4vQEJS7TNb9rA";
	private static String ConsumerSecret = "8Er6J6iI6EQH8jzYsbVrWbgdqFf3WcWBzsRE8Yxzu0";
	private static String AccessToken = "544194759-esvNxnh4p8k8BAo95VteSplM5aVuPhCjQHEjuNuF";
	private static String AccessTokenSecret = "ikNacleByw5U8O7SjF04DWZzTXQIMLLyFBoI4dobk";
	private static final TwitterApi api = new TwitterApi();
	private static DBCollection dbCollection ;
	
	@SuppressWarnings("unchecked")
	public static void main(String[] args) throws UnknownHostException, MongoException {
		api.setOAuth(ConsumerKey, ConsumerSecret, AccessToken, AccessTokenSecret);
		String[] keywords = getKeywords();
		DB db = getMongoDB();
		dbCollection = db.getCollection("streaming");
		for(String kw:keywords){
			String url = String.format(URL,kw,"0");
			while(url!=null){
				JSONObject json = fetchPage(url);
				String[] statusIds = getStatusIds(json);
				Arrays.sort(statusIds,new IDComparator());
				for(String s:statusIds){
					Long id = Long.valueOf(s);
					try {
						Map<String,Object> status = api.getStatus(id);
 						save(id,status);
						Thread.sleep(12*1000);
					} catch (TwitterException e) {
						e.printStackTrace();
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
				if(!hasMore(json)){
					url = null;
					continue;
				}
				String lastId = statusIds[0];
				url = String.format(URL,kw,lastId);
			}
		}
		
	}
	

	public static String[] getAllMatchPattern(String regex,String input){
		Pattern pat = PatternUtils.getPattern(regex);
		Matcher matcher = pat.matcher(input);
		ArrayList result = new ArrayList();
		while(matcher.find()){
			for(int i=0;i<matcher.groupCount();i++){
				result.add(matcher.group(i+1));
			}
		}
		return (String[]) result.toArray(new String[result.size()]);
	}
	
	private static DB getMongoDB() throws UnknownHostException, MongoException{
		 System.out.println("Connecting to DB...");
		 Mongo mongo = new Mongo("137.132.145.75", 27017);
		 DB db = mongo.getDB("zhongyin");
		 db.authenticate("zhongyin", "zhongyin".toCharArray());
		 return db;
	}

	// 137。132.145.75
	// SuperUser,superuser
	//27017
	//EPL ,epl,epl
	//
	
	
	/**
	 * 
	 * fetch next page
	 * 
	 * @param id
	 * @return
	 */
	private static JSONObject fetchPage(String url) {
		HttpPage page = HttpPageLoader.getDefaultPageLoader().download(url);
		String content = page.getContentHtml();
		JSONObject json = JSONObject.fromObject(content);
		return json;
	}

	/**
	 * 
	 * fetch next page
	 * 
	 * @param id
	 * @return
	 */
	private static Document getDocument(String html) {
		Document document = Jsoup.parse(html);
		return document;
	}
	

	/**
	 * select count until now
	 * 
	 * @param count
	 * @return
	 */
	public static void save(Long id,Map<String,Object> data){
		if(!data.containsKey(Constants.FETCH_TIME)){
			data.put(Constants.FETCH_TIME, new Date());
		}
		Object fdate = DateFormat.changeStrToDate(data.get(Constants.FETCH_TIME));
		data.put(Constants.FETCH_TIME, fdate);
		if(data.containsKey(Constants.CREATE_TIME)){
			Object cdate = DateFormat.changeStrToDate(data.get(Constants.CREATE_TIME));
			data.put(Constants.CREATE_TIME, cdate);
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(JSONUtils.map2JSONObj(data));
		dbCollection.findAndModify(query, null, null, false, doc, false, true);
			
	}
	/**
	 * 
	 * fetch next page
	 * 
	 * @param id
	 * @return
	 */
	private static String[] getStatusIds(JSONObject json) {
		if(!json.containsKey("items_html")){
			return null;
		}
		String html = json.getString("items_html");
		return getAllMatchPattern("data-tweet-id=\"(\\d*)\"", html);
	}
	
	private static boolean hasMore(JSONObject json) {
		return json.getBoolean("has_more_items");
	}
	
	private static String[] getKeywords(){
		StringBuffer sb = new StringBuffer();
		sb.append("中印");
		return sb.toString().split(",");
	}
	
	/**
	 * class web data comparator
	 * 
	 * @author zhufb
	 *
	 */
	static class IDComparator<String> implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			 Long a0 = Long.valueOf(arg0.toString());
			 Long a1 = Long.valueOf(arg1.toString());
			 return a0.compareTo(a1);
		 }
	}
}
