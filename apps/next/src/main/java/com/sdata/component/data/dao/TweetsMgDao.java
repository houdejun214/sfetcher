package com.sdata.component.data.dao;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Repository;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.sdata.core.Constants;
import com.sdata.core.data.FieldProcess;

/**
 * save tweets data to mongodb database
 * 
 * @author houdj
 *
 */
@Repository
public class TweetsMgDao extends MongoDao{
	private static final String ID=Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String TweetsCollection="tweets";
	private DBCollection twCollection;
	
	FieldProcess process;
	private UserMgDao userdao = new UserMgDao();
	
	private boolean isSaveUser;
	
	private List<DBObject> tweetsList;
	private List<Map<String,Object>> newtdsList;
	
	@Override
	public void initilize(String host, int port, String dbName) {
		super.initilize(host, port, dbName);
		userdao.initilize(this.m, this.db);
		isSaveUser = true;
		tweetsList = new ArrayList<DBObject>();
		newtdsList = new ArrayList<Map<String,Object>>();
	}

	/**
	 * insert a new tweet 
	 * @param tweet
	 */
	public void saveTweet(Map<String,Object> tweet,FieldProcess fieldProcess){
		this.saveTweet(tweet,fieldProcess,true);
	}
	
	/**
	 * save tweets data without index the data
	 * @param tweet
	 * @param fieldProcess
	 */
	public void saveTweet(Map<String,Object> tweet,FieldProcess fieldProcess,boolean index){
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		if(process == null) this.process = fieldProcess;
		Object id = tweet.get(ID);
		if(id == null ){
			throw new RuntimeException("the property of tweetId is empty!");
		}
		if(!(id instanceof Long || id instanceof UUID)){
			String tweetsId = StringUtils.valueOf(id);
			if("".equals(tweetsId)) throw new RuntimeException("the property of tweetId is empty!");
			id = Long.valueOf(tweetsId);
			tweet.put(ID, id);
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		// check,if has retweet then save else return
		this.saveRetweeted(tweet,index);
		// check,if has user then save else return
		if(isSaveUser){
			this.saveUser(tweet);
		}
		// check if has topic id then save else return
		if(tweet.containsKey(Constants.TWEET_TOPIC_ID)){
			BasicDBObject update = new BasicDBObject();
			if(tweet.containsKey(Constants.TWEET_TOPIC_ID)){
				update.put(Constants.TWEET_TOPIC_ID,tweet.get(Constants.TWEET_TOPIC_ID));
				tweet.remove(Constants.TWEET_TOPIC_ID);
				twCollection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
			}
		}
		//tweet.get(tweetsId);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(tweet);
		doc.remove(ID);
		twCollection.findAndModify(query, null, null, false, new BasicDBObject("$set",doc), false, true);
		//create tweets index
		if(index){
			fieldProcess.solrIndex(tweet);
		}
	}
	
	/**
	 * batch save tweets data and donot save user data,just for crawl tweets by users
	 * this method will creat index for tweets
	 * @param tweet
	 * @param fieldProcess
	 */
	public void batchSaveTweet(Map<String,Object> tweet,FieldProcess fieldProcess, Map<String, Object> newtdMap){
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		if(process == null) this.process = fieldProcess;
		Object id = tweet.get(ID);
		if(id == null ){
			throw new RuntimeException("the property of tweetId is empty!");
		}
		if(!(id instanceof Long || id instanceof UUID)){
			String tweetsId = StringUtils.valueOf(id);
			if("".equals(tweetsId)) throw new RuntimeException("the property of tweetId is empty!");
			id = Long.valueOf(tweetsId);
			tweet.put(ID, id);
		}
		// check,if has retweet then save else return
		this.batchSaveRetweeted(tweet);
		//tweet.get(tweetsId);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(tweet);
		this.batchSave(doc,newtdMap,fieldProcess);
		
	}
	
	/**
	 * batch save datas and build index
	 * @param doc
	 * @param fieldProcess
	 */
	private void batchSave(BasicDBObject doc, Map<String, Object> newtdMap,FieldProcess fieldProcess){
		synchronized(tweetsList){
			tweetsList.add(doc);
			if(newtdMap!=null){
				newtdsList.add(newtdMap);
			}
		}
		if(tweetsList.size()>=500){
			if(twCollection==null){
				twCollection = this.getDBCollection(TweetsCollection);
			}
			synchronized(tweetsList){
				twCollection.insert(tweetsList);
				for(DBObject tweet:tweetsList){
					fieldProcess.solrIndex(tweet.toMap());
				}
				tweetsList.clear();
			}
			synchronized(newtdsList){
				for(Map<String,Object> map:newtdsList){
					Object id = map.get("id");
					Date newtd = (Date)map.get("newtd");
					userdao.updateNewTweetsDate(id, newtd);
				}
				newtdsList.clear();
			}
		}
	}
	
	
	/**
	 * insert tweet into the database, without update.
	 * 
	 * @param tweet
	 * @param fieldProcess
	 */
	public boolean insertTweet(Map<String,Object> tweet,FieldProcess fieldProcess){
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		if(process == null) this.process = fieldProcess;
		Object id = tweet.get(ID);
		if(id == null ){
			throw new RuntimeException("the property of tweetId is empty!");
		}
		if(!(id instanceof Long || id instanceof UUID)){
			String tweetsId = StringUtils.valueOf(id);
			if("".equals(tweetsId)) throw new RuntimeException("the property of tweetId is empty!");
			id = Long.valueOf(tweetsId);
			tweet.put(ID, id);
		}
		// check, if has retweet then save else return
		this.insertRetweeted(tweet,fieldProcess);
		// check,if has user then save else return
		this.insertUser(tweet);
		BasicDBObject query = new BasicDBObject(ID,id);
		if(twCollection.count(query)==0){
			//tweet.get(tweetsId);
			BasicDBObject doc = new BasicDBObject();
			doc.putAll(tweet);
			twCollection.insert(doc);
			return true;
		}
		return false;
	}
	
	
	/**
	 * insert a new user
	 * @param tweet
	 */
	public void saveUser( Map<String,Object> tweet){
		if(!tweet.containsKey(Constants.TWEET_USER)){
			return;
		}
		// change date string to date
		Map<String, Object> user = (Map<String, Object>)tweet.get(Constants.TWEET_USER);
		tweet.remove(Constants.TWEET_USER);
		Object uid = user.get(Constants.OBJECT_ID);
		if(uid==null){
			uid = user.get(Constants.USER_ID);
		}
		if(uid==null){
			throw new RuntimeException("user id is empty!");
		}
		user.put(Constants.OBJECT_ID, uid);
		tweet.put("uid", uid);
		tweet.put("uname", user.get("name"));
		tweet.put("sname", user.get("sname"));
		// save user
		userdao.saveUser(user);
	}
	
	/**
	 * insert a new user
	 * @param tweet
	 */
	public void insertUser( Map<String,Object> tweet){
		if(!tweet.containsKey(Constants.TWEET_USER)){
			return;
		}
		// change date string to date
		Map<String, Object> user = (Map<String, Object>)tweet.get(Constants.TWEET_USER);
		tweet.remove(Constants.TWEET_USER);
		Object uid = user.get(Constants.OBJECT_ID);
		if(uid==null){
			uid = user.get(Constants.USER_ID);
		}
		if(uid==null){
			throw new RuntimeException("user id is empty!");
		}
		user.put(Constants.OBJECT_ID, uid);
		tweet.put("uid", uid);
		tweet.put("uname", user.get("name"));
		tweet.put("sname", user.get("sname"));
		// save user
		userdao.insertUser(user);
	}
	
	/**
	 * save a new retweet
	 * @param tweet
	 * @param index 
	 */
	public void saveRetweeted( Map<String,Object> tweet, boolean index){
		if(!tweet.containsKey(Constants.TWEET_RETWEETED)){
			return;
		}
		// parse
		Map<String, Object> retweet = (Map<String, Object>)tweet.get(Constants.TWEET_RETWEETED);
		String retid = String.valueOf(retweet.get(Constants.TWEET_ID));
		if(retid == null)
			throw new RuntimeException("the property of tweetId is empty!");
		tweet.put(Constants.TWEET_RETWEETED_ID, Long.parseLong(retid));
		tweet.remove(Constants.TWEET_RETWEETED);
		
		// save retweet
		retweet.put(Constants.OBJECT_ID, Long.parseLong(retid));
		this.saveTweet(retweet,this.process,index);
	}
	
	public void batchSaveRetweeted( Map<String,Object> tweet){
		if(!tweet.containsKey(Constants.TWEET_RETWEETED)){
			return;
		}
		// parse
		Map<String, Object> retweet = (Map<String, Object>)tweet.get(Constants.TWEET_RETWEETED);
		String retid = String.valueOf(retweet.get(Constants.TWEET_ID));
		if(retid == null)
			throw new RuntimeException("the property of tweetId is empty!");
		tweet.put(Constants.TWEET_RETWEETED_ID, Long.parseLong(retid));
		tweet.remove(Constants.TWEET_RETWEETED);
		// save retweet
		retweet.put(Constants.OBJECT_ID, Long.parseLong(retid));
		this.batchSaveTweet(retweet,this.process,null);
	}
	
	/**
	 * insert a new retweet
	 * @param tweet
	 * @param index 
	 */
	public void insertRetweeted( Map<String,Object> tweet, FieldProcess fieldProcess){
		if(!tweet.containsKey(Constants.TWEET_RETWEETED)){
			return;
		}
		// parse
		Map<String, Object> retweet = (Map<String, Object>)tweet.get(Constants.TWEET_RETWEETED);
		String retid = String.valueOf(retweet.get(Constants.TWEET_ID));
		if(retid == null)
			throw new RuntimeException("the property of tweetId is empty!");
		tweet.put(Constants.TWEET_RETWEETED_ID, Long.parseLong(retid));
		tweet.remove(Constants.TWEET_RETWEETED);
		// save retweet
		retweet.put(Constants.OBJECT_ID, Long.parseLong(retid));
		retweet.put(Constants.FETCH_TIME, new Date());
		this.insertTweet(retweet,fieldProcess);
	}
	
	/**
	 * delete a tweet by id
	 * @param tweetsId
	 */
	public void deleteTweet(String tweetsId) {
		if(StringUtils.isEmpty(tweetsId)){
			throw new RuntimeException("the property of tweetsId is empty!");
		}
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,Long.valueOf(tweetsId));
		twCollection.findAndRemove(query);
	}
	
	/**
	 * delete a tweet by id
	 * @param tweetsId
	 */
	public boolean deleteTweet_Dfrom(String tweetsId) {
		if(StringUtils.isEmpty(tweetsId)){
			throw new RuntimeException("the property of tweetsId is empty!");
		}
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,Long.valueOf(tweetsId));
		query.put("dfrom", "import");
		twCollection.findAndRemove(query);
		BasicDBObject query2 = new BasicDBObject(ID,Long.valueOf(tweetsId));
		query2.put("dfrom", "import");
		if(twCollection.count(query2)==0){
			return true;
		}
		return false;
	}
	
	/**
	 * query a tweet by id
	 * 2012-7-1  geyong
	 * @param tweetsId
	 */
	public DBObject queryTweet(String tweetsId) {
		if(StringUtils.isEmpty(tweetsId)){
			throw new RuntimeException("the property of tweetsId is empty!");
		}
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		BasicDBObject query = new BasicDBObject(ID,Long.valueOf(tweetsId));
		DBObject res = twCollection.findOne(query);
		return res;
	}
	
	/**
	 * check whether the tweet is exists
	 * @param tweetsId
	 * @return
	 */
	public boolean isTweetExists(String tweetsId){
		if(StringUtils.isEmpty(tweetsId)){
			throw new RuntimeException("Tweet Id is empty");
		}
		BasicDBObject query = new BasicDBObject(ID,Long.valueOf(tweetsId));
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		boolean hasNext = twCollection.find(query).hasNext();
		return hasNext;
	}
	
	public boolean isTweetExists(Long tweetsId){
		if(tweetsId==null){
			throw new RuntimeException("Tweet Id is empty");
		}
		BasicDBObject query = new BasicDBObject(ID,tweetsId);
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		boolean hasNext = twCollection.find(query).hasNext();
		return hasNext;
	}

	public boolean isSaveUser() {
		return isSaveUser;
	}

	public void setSaveUser(boolean isSaveUser) {
		this.isSaveUser = isSaveUser;
	}
	
	
	public void addTopicId(Object id,Object topicId){
		if(twCollection==null){
			twCollection = this.getDBCollection(TweetsCollection);
		}
		Long lid = Long.valueOf(id.toString());
		BasicDBObject query = new BasicDBObject(ID,lid);
		BasicDBObject update = new BasicDBObject();
		update.put(Constants.TWEET_TOPIC_ID,topicId);
		twCollection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
	}
	
}
