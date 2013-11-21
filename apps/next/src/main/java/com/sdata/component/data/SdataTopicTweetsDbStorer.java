package com.sdata.component.data;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.TopicMgDao;
import com.sdata.component.data.dao.TopicTimeLineDao;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;
import com.sdata.core.util.ApplicationContextHolder;

/**
 * store tweets data which contain twitter tweets and weibo tweets
 * 
 * @author houdj
 *
 */
public class SdataTopicTweetsDbStorer extends SdataStorer {
	
	private static final String ID= Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataTopicTweetsDbStorer");
	private TweetsMgDao tweetDao;
	private TopicMgDao topicDao;
	private TopicTimeLineDao topicTimeLineDao;
	private FieldProcess fieldProcess;
	private FieldProcess topicIndexProcess ;

	public SdataTopicTweetsDbStorer(Configuration conf,RunState state) throws UnknownHostException, MongoException{
		this.setConf(conf);
		this.tweetDao = new TweetsMgDao();
		this.topicDao = new TopicMgDao();
		this.topicTimeLineDao = new TopicTimeLineDao();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.tweetDao.initilize(host, port, dbName);
		this.topicDao.initilize(host, port, dbName);
		this.topicTimeLineDao.initilize(host, port, dbName);
		this.state = state;
		String path = this.getConf("topicIndexPath");
		fieldProcess = new FieldProcess(conf);
		topicIndexProcess = new FieldProcess(conf,path);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		if(datum!=null ){
			try {
				Map<String, Object> topicdata = datum.getMetadata();
				if(topicdata==null){
					return;
				}
				String locKey = "loc";
				if(topicdata.containsKey(Constants.TOPIC_LOCATION)){
					locKey = Constants.TOPIC_LOCATION;
				}
				Object topicId = topicdata.get(ID);
				if(topicId ==null ){
					topicId = topicdata.get(Constants.TOPIC_ID);
					if(topicId==null){
						String name = (String)topicdata.get(Constants.TOPIC_NAME);
						String locName =(String) ((Map)topicdata.get(locKey)).get("locname");
						if(StringUtils.isEmpty(name) || StringUtils.isEmpty(locName)){
							throw new RuntimeException("the property of topic id，name is empty!");
						}
						topicId = UUIDUtils.getMd5UUID(name+locName);
					}
				}
				
				//1、save tweet infomation
				List<Object> tweetsList = topicdata.get(Constants.TOPIC_TWEETS)==null?new ArrayList<Object>():(List<Object>)topicdata.get(Constants.TOPIC_TWEETS);
				int tSize = tweetsList.size();
				boolean fetched = false;
				List<Object> tweetsIds = new ArrayList<Object>();
				for(int i=0;i<tweetsList.size();i++){
					Object object = tweetsList.get(i);
					if(object instanceof Map){
						Map<String ,Object> tweetJSONObj = (Map<String ,Object>)object;
						tweetJSONObj.put(Constants.OBJECT_ID, tweetJSONObj.get(Constants.TWEET_ID));
						Map<String, Object> tweetData = fieldProcess.fieldReduce(tweetJSONObj);
						tweetData.put(Constants.FETCH_TIME, new Date());
						tweetData.put(Constants.TWEET_TOPIC_ID, topicId);
						try {
							tweetDao.saveTweet(tweetData,fieldProcess,false);
						} catch (Exception e) {
							logFaileMessage(e,"save tweet failed");
						}
						tweetsIds.add(StringUtils.valueOf(tweetData.get(ID)));
						fetched = true;
					}else if(object instanceof String){
						tweetsIds.add(object.toString());
					}
				}
//			log.info("*********tweets in topic【"+topicdata.get("name")+"】save count:"+tSize+"********");
				//2、get the relationship between topic and tweet
				topicdata.remove(Constants.TOPIC_TWEETS);
				if(fetched){
					topicdata.put(Constants.TOPIC_TWEETS_FETCHED_LIST, tweetsIds);
				}else{
					topicdata.put(Constants.TOPIC_TWEETS_LIST, tweetsIds);
				}
				//3、save topic infomation and the relationship between topic and tweet
				//long sequenceId = state.getNextSequenceId();
				//topicdata.put(ID, sequenceId);
				Map<String, Object> topic = topicIndexProcess.fieldReduce(topicdata);
				//save time line
				//topic.put(Constants.FETCH_TIME, new Date());
				String state = String.valueOf(topic.get(Constants.TOPIC_STATE));
				
				
				if(!Constants.TOPIC_STATE_END.equals(state)){
					boolean b = true;
					if(!topic.containsKey(Constants.FETCH_TIME)){
						b = false;
						topic.put(Constants.FETCH_TIME, new Date());
					}
					//如果前面的代码已经处理了_id，则此处保留好_id
					Object _id = null;
					if(topic.containsKey(Constants.OBJECT_ID)){
						_id = topic.get(Constants.OBJECT_ID);
						topic.remove(Constants.OBJECT_ID);
					}
					topicTimeLineDao.save(topic);
					if(!b){
						topic.remove(Constants.FETCH_TIME);
					}
					if(_id!=null){
						topic.put(Constants.OBJECT_ID, _id);
					}
				};
				
				//save topic
				topicDao.save(topic,topicIndexProcess);
				log.info("*********tweets in topic【"+topicdata.get("name")+"】  state "+state+" save count:"+tSize+"********");
			} catch (Exception e) {
				logFaileMessage(e,"save failed");
				log.info("*********something error in SdataTopicTweetsDbStorer.save()********");
				throw e;
			}
		}
	}

	protected void logFaileMessage(Exception e,String preMessage){
		String msg = preMessage+" : "+e.getMessage();
		Throwable cause = e.getCause();
		if(cause!=null){
			msg+="\r\n cause by :"+cause.getClass()+"\n"+cause.getMessage();
		}
		log.info(msg);
	}

}
