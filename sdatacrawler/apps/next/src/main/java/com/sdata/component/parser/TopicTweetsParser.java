package com.sdata.component.parser;


import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.mongodb.DBObject;
import com.sdata.component.data.dao.TopicMgDao;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.site.WeiboTweetAPI;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.parser.SdataParser;


/**
 * @author zhufb
 *
 */
public class TopicTweetsParser extends SdataParser{
	
	public static final String weiboHost="http://topic.weibo.com";
	TweetsMgDao dao = new TweetsMgDao();
	public TopicMgDao topicDao = new TopicMgDao();
	public TweetsMgDao tweetsDao = new TweetsMgDao();
	public static final Log log = LogFactory.getLog("SdataCrawler.WeiboTopicParser");
	WeiboTweetAPI weiboTweet;
	private int maxRequest;
	private String fetchStatus;
	private int fetchDays;
	
	public TopicTweetsParser(Configuration conf,RunState state) {
		setConf(conf);
		setRunState(state);
		String host = conf.get("mongoHost");
		int port = conf.getInt("mongoPort",27017);
		String dbName = conf.get("mongoDbName");
		topicDao.initilize(host,port,dbName);
		weiboTweet = new WeiboTweetAPI(conf,state);
		this.tweetsDao.initilize(host, port, dbName);
		dao.initilize(host, port, dbName);
		maxRequest =conf.getInt("maxRequestHour", 150);
		fetchStatus =conf.get("fetchStatus", "start");
		fetchDays =conf.getInt("fetchDays", 150);
	}
	
	public List<FetchDatum> initFtlTopicList(){
		List<FetchDatum> fetchDatumList = new ArrayList<FetchDatum>();
		log.fatal("getStartTopicList,startTime:"+(new Date()).toString());
		List<?> stList = getStartTopicList();
		log.fatal("getStartTopicList,endTime:"+(new Date()).toString());
		int curFetchNum= 0;
		int lastFecthTotalNum=0;
		for(int i=0;i<stList.size();i++){
			Map<String, Object> topic= ((DBObject)stList.get(i)).toMap();
			List<?> tweetsList=(List<?>)topic.get(Constants.TOPIC_TWEETS_LIST);
			//fetched Tweets List
			List<String> ftList=(List<String>)topic.get(Constants.TOPIC_TWEETS_FETCHED_LIST);
			if(ftList==null){
				ftList = new ArrayList<String>();
			}
			tweetsList.removeAll(ftList);
			for(int j=0;j<tweetsList.size();j++){
				String tweetsId=(String)tweetsList.get(j);
				if(tweetsDao.isTweetExists(tweetsId)){
					ftList.add(tweetsId);
				}
			}
			topic.put(Constants.TOPIC_TWEETS_FETCHED_LIST, ftList);
			//save tweetsFtl
			topicDao.updateTopicFtl(topic);
			List<String> currList=new ArrayList<String>(); 
			curFetchNum=(maxRequest-lastFecthTotalNum)/(stList.size()-i);
			lastFecthTotalNum += curFetchNum;
			for(int m=0;m<curFetchNum;m++){
				if(m<tweetsList.size()){
					currList.add((String)tweetsList.get(m));
				}
			}
			topic.put(Constants.TOPIC_TWEETS_CURR_FETCHED_LIST, currList);
			FetchDatum fd= new FetchDatum();
			fd.setId(topic.get("id").toString());
			fd.setMetadata(topic);
			fetchDatumList.add(fd);
		}
		log.fatal("end initFtlTopicList,endTime:"+(new Date()).toString());
		return fetchDatumList;
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
}
