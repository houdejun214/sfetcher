package com.sdata.component.parser;


import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.PatternUtils;
import com.mongodb.DBObject;
import com.sdata.component.data.dao.TopicMgDao;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.site.WeiboTweetAPI;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.parser.SdataParser;
import com.sdata.core.util.ApplicationContextHolder;
import com.sdata.core.util.WebPageDownloader;


/**
 * @author zhufb
 *
 */
public class TopicHotTweetsParser extends SdataParser{

	public static final String weiboHost="http://topic.weibo.com";
	public static final String topicUrl = "http://huati.weibo.com/aj_topic/list?all=1&pic=0&hasv=0&atten=0&prov=0&city=0&_t=0&order=hot";
	String url_str = "?xsort=hot&pos=0&lable_t=tips#topic";
	public TopicMgDao topicDao = new TopicMgDao();
	public TweetsMgDao tweetsDao = new TweetsMgDao();
	public static final Log log = LogFactory.getLog("SdataCrawler.TopicHotTweetsParser");
	WeiboTweetAPI weiboTweet;
	private int maxRequest;
	private String fetchStatus;
	private int fetchDays;
	
	public TopicHotTweetsParser(Configuration conf,RunState state) {
		setConf(conf);
		setRunState(state);
		String host = conf.get("mongoHost");
		int port = conf.getInt("mongoPort",27017);
		String dbName = conf.get("mongoDbName");
		topicDao.initilize(host,port,dbName);
		weiboTweet = new WeiboTweetAPI(conf,state);
		this.tweetsDao.initilize(host, port, dbName);
		maxRequest =conf.getInt("maxRequestHour", 150);
		fetchStatus =conf.get("fetchStatus", "start");
		fetchDays =conf.getInt("fetchDays", 150);
	}
	
	public List<FetchDatum> initFtlTopicList(){
		List<FetchDatum> fetchDatumList = new ArrayList<FetchDatum>();
		log.warn("getStartTopicList,startTime:"+(new Date()).toString());
		List<?> stList = getStartTopicList();
		log.warn("getStartTopicList,endTime:"+(new Date()).toString());
		int curFetchNum= 0;
		for(int i=0;i<stList.size();i++){
			Map<String, Object> topic= ((DBObject)stList.get(i)).toMap();
			String url = ((String)topic.get(Constants.TOPIC_URL)).concat(url_str);
			Document doc = this.fetchPage(url);
			if(doc == null) {
				continue;
			}
			// fetch weibo topic's tweets list
    		JSONArray array = new JSONArray();
    		JSONArray ftarray = new JSONArray();
			List<JSONObject> tweetslist = new ArrayList<JSONObject>();
    		//fetch hot topics
			int length = 0;
//			boolean newTemp = false;
			String keywords = null;
    		//Elements pages = doc.select(".list div a:not(.current)");
    		//if(pages == null||pages.size() ==0){
			keywords =  URLEncoder.encode(unescape(PatternUtils.getMatchPattern("\"keyword\":\"(.*?)\"", doc.toString(), 1)));
			length = super.getConfInt(Constants.WEIBO_FETCH_PAGE_NUM,5);
//			newTemp = true;
			doc = this.parseHtmlDocument(WeiboTopicPageDownloader.download(topicUrl.concat("&p=1").concat("&keyword=").concat(keywords)));
//    		}else{
//    			length = pages.size();
//    			Collections.sort(pages,new ComparatorPages());
//    		}
    		
    		for(int p=0;p<length&&doc!=null;p++){
    			Elements tweets = doc.select("dl[mid],.list_feed_li");
    			Iterator<Element> iterator2 = tweets.iterator();
				while(iterator2.hasNext()){
					Element tweet = iterator2.next();
					String id = tweet.attr("mid");
					if(StringUtils.isEmpty(id)){
						id = tweet.attr("list-data").substring(4);
					}
					String[] split = id.split("&");
					id = split[0];
					array.add(id);
					if(!tweetsDao.isTweetExists(id)){
						if(curFetchNum>=maxRequest){
							break;
						}
						try{
							JSONObject fetchTweet = this.fetchTweet(id);
							tweetslist.add(fetchTweet);
							curFetchNum++;
						}catch(Exception e){
							e.printStackTrace();
							continue;
						}
					}
					ftarray.add(id);
				}
				if(curFetchNum>=maxRequest){
					break;
				}
//				if(newTemp){
					doc = this.parseHtmlDocument(WeiboTopicPageDownloader.download(topicUrl.concat("&p=").concat(String.valueOf(p+2).concat("&keyword=").concat(keywords))));
//				}else{
//					doc = this.fetchPage(weiboHost + pages.get(p).attr("href"));
//				}
    		}
    		topic.put(Constants.TOPIC_TWEETS, tweetslist);
    		topic.put(Constants.TOPIC_TWEETS_FETCHED_LIST, ftarray);
    		FetchDatum fd= new FetchDatum();
    		fd.setId(topic.get("id").toString());
    		fd.setMetadata(topic);
    		fetchDatumList.add(fd);
    		log.warn("topic:"+topic.get(Constants.TOPIC_NAME)+"down hot tweets: "+ftarray.size());
    		if(curFetchNum>=maxRequest){
				break;
			}
		}
		log.warn("end initFtlTopicList,endTime:"+(new Date()).toString());
		return fetchDatumList;
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
	
	private Document fetchPage(String url) {
		String content = WebPageDownloader.download(url);
		Document doc = parseHtmlDocument(content);
		return doc;
	}
	/**
	 * init topic list info 
	 * 
	 * @param TopicTweetsParser
	 * @return
	 */
	private List<?> getStartTopicList(){
		return  topicDao.query(fetchDays);
	}
	/**
	 * 
	 * fetch one single tweet
	 * 
	 * @param id
	 * @return
	 */
	public JSONObject fetchTweet(String id) {
		 return weiboTweet.fetchOneTweet(id);
	}
	

	class ComparatorPages implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			try{
				String href1 = ((Element)arg0).attr("href");
				String href2 = ((Element)arg1).attr("href");
				Integer page1 = Integer.valueOf(href1.substring(href1.lastIndexOf("=")+1));
				Integer page2 = Integer.valueOf(href2.substring(href2.lastIndexOf("=")+1));
				return page1.compareTo(page2);
			}catch(Exception e){
				return 1;
			}
			
		 }
	}
}
