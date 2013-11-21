package com.sdata.component.data;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.mongodb.MongoException;
import com.sdata.component.data.dao.FamousMgDao;
import com.sdata.component.data.dao.FamousTimeLineDao;
import com.sdata.component.data.dao.TweetsMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;
import com.sdata.core.util.ApplicationContextHolder;

/**
 * store weibo 【famous,user,tweets】 data
 * 
 * @author zhufb
 *
 */
public class SdataFamousTweetsDbStorer extends SdataStorer {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataFamousTweetsDbStorer");

	private TweetsMgDao tweetDao;
	private UserMgDao userDao;
	private FamousMgDao famousDao;
	private FamousTimeLineDao famousTimelineDao;
	private FieldProcess fieldProcess ;
	private FieldProcess famousIndexProcess ;
	private FieldProcess userProcess ;
	public SdataFamousTweetsDbStorer(Configuration conf,RunState state) throws UnknownHostException, MongoException{
		this.setConf(conf);
		this.tweetDao = new TweetsMgDao();
		this.userDao = new UserMgDao();
		this.famousDao = new FamousMgDao();
		this.famousTimelineDao = new FamousTimeLineDao();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.tweetDao.initilize(host, port, dbName);
		this.userDao.initilize(host, port, dbName);
		this.famousDao.initilize(host, port, dbName);
		this.famousTimelineDao.initilize(host, port, dbName);
		this.state = state;
		String path = this.getConf("famousIndexPath");
		String userPath = this.getConf("userPath");
		fieldProcess = new FieldProcess(conf);
		famousIndexProcess = new FieldProcess(conf,path);
		userProcess = new FieldProcess(conf,userPath);
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public void save(FetchDatum datum) throws Exception {
		try {
			Map<String, Object> famous = datum.getMetadata();
			// save famous's user
			Map<String,Object> user = (Map<String,Object>)famous.remove(Constants.FAMOUS_ACCOUNT);
			
			//save tweet infomation,user's tweets
			List<Map<String,Object>> tweetsList = (List<Map<String,Object>>)famous.get(Constants.FAMOUS_TWEETS);
			if(tweetsList == null) tweetsList = new ArrayList<Map<String,Object>>();
			List<Object> userTweets = new ArrayList<Object>();
			for(int i=0;i<tweetsList.size();i++){
				Map<String ,Object> tweetJSONObj = tweetsList.get(i);
				String tweetId = String.valueOf(tweetJSONObj.get(Constants.TWEET_ID));
				if(StringUtils.isEmpty(tweetId)){
					continue;
				}
				Map<String, Object> tweet = fieldProcess.fieldReduce(tweetJSONObj);
				tweet.put(Constants.OBJECT_ID, Long.parseLong(tweetId));
				tweet.put(Constants.FETCH_TIME, new Date());
				tweetDao.saveTweet(tweet,fieldProcess,false);
				userTweets.add(Long.parseLong(tweetId));
			}
			//save user's tweets 
			famous.remove(Constants.FAMOUS_TWEETS);
			if(userTweets.size()>0){
				famous.put(Constants.FAMOUS_TWEETS, userTweets);
//				if(!famous.containsKey(Constants.FAMOUS_NEWEST_TWEET)){
//					if(!famous.containsKey(Constants.FAMOUS_NEWEST_TWEET)){
				Object newestTweet = this.getNewestTweet(userTweets);
				famous.put(Constants.FAMOUS_NEWEST_TWEET, newestTweet);
//					}
//				}
			}
			famous.remove(Constants.FAMOUS_TWEETS_FLIST);
			//user's friend
			List<Map<String,Object>> friends = (List<Map<String,Object>>)famous.get(Constants.FAMOUS_FRIENDS);
			if(friends == null) friends = new ArrayList<Map<String,Object>>();
			List<Object> userFriends = new ArrayList<Object>();
			for(int i=0;i<friends.size();i++){
				Map<String ,Object> friendJson = friends.get(i);
				String uid = String.valueOf(friendJson.get(Constants.USER_ID));
				if(StringUtils.isEmpty(uid)){
					continue;
				}
				Map<String, Object> friend = userProcess.fieldReduce(friendJson);
				friend.put(Constants.OBJECT_ID, Long.parseLong(uid));
				userDao.saveUser(friend);
				userFriends.add(Long.parseLong(uid));
			}
			//save user and star 
			famous.remove(Constants.FAMOUS_FRIENDS);
			if(userFriends.size()>0) {
				famous.put(Constants.FAMOUS_FRIENDS, userFriends);
			}
			//save IdolList
			ArrayList<Object> idoll = new ArrayList<Object>();
			if(famous.containsKey(Constants.FAMOUS_IDOLLIST)){
				Object obj = famous.get(Constants.FAMOUS_IDOLLIST);
				if(null!=obj){
					List<Map<String,Object>> IdolList = (ArrayList<Map<String,Object>>)obj;
					for(Map<String,Object> idol :IdolList){
						if(null==idol){
							log.error("one of famous's friend is null");
							continue;
						}
						idol = userProcess.fieldReduce(idol);
						Object name = idol.get(Constants.FAMOUS_NAME);
						if(null==name){
							log.error("famous's friend name is null");
							continue;
						}
						Object id = UUIDUtils.getMd5UUID(String.valueOf(name));
						idol.put(Constants.OBJECT_ID, id);
						userDao.saveUser(idol);
						idoll.add(id);
					}
				}
				famous.remove(Constants.FAMOUS_IDOLLIST);
				if(idoll.size()>0){
					famous.put(Constants.FAMOUS_FRIENDS, idoll);
				}
			}
			famous = famousIndexProcess.fieldReduce(famous);
			//user
			if(user != null){
				user = userProcess.fieldReduce(user);
				userDao.saveUser(user);
			}
			//timeline
			famous.remove(Constants.OBJECT_ID);
			famousTimelineDao.save(famous);
			famous.put(Constants.OBJECT_ID, famous.get(Constants.FAMOUS_ID));
			//famous
			famousDao.save(famous,famousIndexProcess);
			log.info("*********famous【"+famous.get(Constants.FAMOUS_ORDER)+"."+famous.get(Constants.FAMOUS_NAME)+"】 save friends count:"+ userFriends.size()+",save idolist count:"+idoll.size()+",save tweets count:"+userTweets.size() +"********");
		} catch (Exception e) {
			logFaileMessage(e);
			throw e;
		}
	}
	
	/**
	 * merge dbTweets deleted to userTweets
	 * 
	 * @param userTweets
	 * @param dbTweets
	 */
	@SuppressWarnings("unchecked")
	protected Object getNewestTweet(List<Object> userTweets){
		 ComparatorMap c = new ComparatorMap();
		 Collections.sort(userTweets, c);
		 return userTweets.size()>0?userTweets.get(0):null;
	}

	protected void logFaileMessage(Exception e){
		String msg = "save failed : "+e.getMessage();
		Throwable cause = e.getCause();
		if(cause!=null){
			msg+="\r\n cause by :"+cause.getClass()+"\n"+cause.getMessage();
		}
		log.info(msg);
	}
	//desc 
	class ComparatorMap implements Comparator{
		 public int compare(Object arg0, Object arg1) {
			Long id1 = (Long) arg0;
			Long id2 = (Long) arg1;
			return id2.compareTo(id1);
		 }
	}

}
