package com.sdata.component.data;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoException;
import com.sdata.component.data.dao.TopicMgDao;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;

/**
 * store tweets data which contain twitter tweets and weibo tweets
 * 
 * @author houdj
 *
 */
public class SdataTopicTweetsByTlDbStorer extends SdataStorer {
	
	private static final String ID= Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataTopicTweetsByTlDbStorer");
	private TweetsMgDao tweetDao;
	private TopicMgDao topicDao;
	private FieldProcess fieldProcess ;

	public SdataTopicTweetsByTlDbStorer(Configuration conf,RunState state) throws UnknownHostException, MongoException{
		this.setConf(conf);
		this.tweetDao = new TweetsMgDao();
		this.topicDao = new TopicMgDao();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.tweetDao.initilize(host, port, dbName);
		this.topicDao.initilize(host, port, dbName);
		this.state = state;
		fieldProcess = new FieldProcess(conf);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		try {
			Map<String, Object> topicdata = datum.getMetadata();
			//1、save tweet infomation
			List<Object> tweetsList = topicdata.get(Constants.TOPIC_TWEETS)==null?new ArrayList<Object>():(List<Object>)topicdata.get(Constants.TOPIC_TWEETS);
			List<Object>  tweetsIds = new ArrayList<Object>();
			for(int i=0;i<tweetsList.size();i++){
				 Object object = tweetsList.get(i);
				 if(object instanceof Map){
					 Map<String ,Object> tweetJSONObj = (Map<String ,Object>)object;
//					 tweetJSONObj.put(Constants.OBJECT_ID, tweetJSONObj.get(Constants.TWEET_ID));
					 Map<String, Object> tweetData = fieldProcess.fieldReduce(tweetJSONObj);
					 tweetData.put(Constants.FETCH_TIME, new Date());
					 tweetDao.saveTweet(tweetData,fieldProcess,false);
					 tweetsIds.add(String.valueOf(tweetData.get(ID)));
				 }
			}
			topicdata.put(Constants.TOPIC_TWEETS_FETCHED_LIST, tweetsIds);
			topicDao.updateTopicFtl(topicdata);
			
			log.info("*********tweets in topic【"+topicdata.get("name")+"】save count:"+tweetsList.size()+"********");
		} catch (Exception e) {
			logFaileMessage(e);
			log.info("*********something error in SdataTopicTweetsDbStorer.save()********");
			log.error(e.getMessage());
			e.printStackTrace();
			throw e;
		}
	}

	protected void logFaileMessage(Exception e){
		String msg = "save failed : "+e.getMessage();
		Throwable cause = e.getCause();
		if(cause!=null){
			msg+="\r\n cause by :"+cause.getClass()+"\n"+cause.getMessage();
		}
		log.info(msg);
	}

}
