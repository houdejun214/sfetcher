package com.sdata.component.data.dao;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Repository;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.data.FieldProcess;

/**
 * save tweets data to mongodb database
 * 
 * @author qmm
 * 
 */
@Repository
public class TopicMgDao extends MongoDao {
	private static final String ID = Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String TopicCollection = "topics";
	private static final String tweetList = "tl";
	private DBCollection collection;
	private static final int maxIdNum = 500000;

	private TweetsMgDao tweetsdao = new TweetsMgDao();
	
	@Override
	public void initilize(String host, int port, String dbName) {
		super.initilize(host, port, dbName);
		tweetsdao.initilize(this.m, this.db);
	}
	
	/**
	 * insert a new topic to DB
	 * 
	 * @param topic
	 * @author qiumm
	 */
	public void save(Map<String, Object> topic,FieldProcess topicIndexProcess ) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		Object id = topic.get(ID);
		if(id ==null ){
			id = topic.get(Constants.TOPIC_ID);
			if(id==null){
				String name = (String)topic.get(Constants.TOPIC_NAME);
				String locName =(String) ((Map)topic.get("loc")).get("locname");
				if(StringUtils.isEmpty(name) || StringUtils.isEmpty(locName)){
					throw new RuntimeException("the property of topic id，name is empty!");
				}
				id = UUIDUtils.getMd5UUID(name+locName);
				topic.put(ID, id);
			}
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		BasicDBObject update = new BasicDBObject();
		if(topic.containsKey(tweetList)){
			update.put(tweetList,new BasicDBObject("$each",topic.get(tweetList)));
			topic.remove(tweetList);
			collection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
		}
		BasicDBObject update2 = new BasicDBObject();
		if(topic.containsKey(Constants.TOPIC_TWEETS_FETCHED_LIST)){
			DBObject topicInDB = collection.findOne(query);
			if(topicInDB!=null){
				Object ftl =topicInDB.get(Constants.TOPIC_TWEETS_FETCHED_LIST);
				BasicDBList IdsList = (BasicDBList)ftl;
				if(IdsList!=null && IdsList.size()>maxIdNum){
					while(IdsList.size()>maxIdNum){
						Object removeId = IdsList.remove(0);
						tweetsdao.addTopicId(removeId, id);
					}
					for(Object tid:(List<Object>)topic.get(Constants.TOPIC_TWEETS_FETCHED_LIST)){
						IdsList.add(tid);
					}
					update2.put(Constants.TOPIC_TWEETS_FETCHED_LIST,IdsList);
					topic.remove(Constants.TOPIC_TWEETS_FETCHED_LIST);
					collection.findAndModify(query, null, null, false, new BasicDBObject("$set",update2), false, true);
				}else{
					update2.put(Constants.TOPIC_TWEETS_FETCHED_LIST,new BasicDBObject("$each",topic.get(Constants.TOPIC_TWEETS_FETCHED_LIST)));
					topic.remove(Constants.TOPIC_TWEETS_FETCHED_LIST);
					collection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update2), false, true);
				}
			}else{
				update2.put(Constants.TOPIC_TWEETS_FETCHED_LIST,new BasicDBObject("$each",topic.get(Constants.TOPIC_TWEETS_FETCHED_LIST)));
				topic.remove(Constants.TOPIC_TWEETS_FETCHED_LIST);
				collection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update2), false, true);
			}
		}
		
		BasicDBObject doc = new BasicDBObject();
		topic.remove(ID);
		doc.putAll(topic);
		collection.findAndModify( query, null, null, false, new BasicDBObject("$set",doc), false, true );
		topic.put(ID, id);
		if(id instanceof UUID){
			topic.put(ID, UUIDUtils.encode((UUID)id));
		}
		topic.put("origin", CrawlAppContext.conf.get(Constants.SOURCE));
		topicIndexProcess.solrIndex(topic);
	}
	
	/**
	 * insert a new topic to DB
	 * 
	 * @param topic
	 * @author qiumm
	 */
	public void updateTopicFtl(Map<String, Object> topic) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		Object id = topic.get(ID);
		if(id ==null ){
			id = topic.get(Constants.TOPIC_ID);
			if(id==null){
				String name = (String)topic.get(Constants.TOPIC_NAME);
				if(StringUtils.isEmpty(name)){
					throw new RuntimeException("the property of topic id，name is empty!");
				}
				id = UUIDUtils.getMd5UUID(name);
				topic.put(ID, id);
			}
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		BasicDBObject update = new BasicDBObject();
		if(topic.containsKey(Constants.TOPIC_TWEETS_FETCHED_LIST)){
			update.put(Constants.TOPIC_TWEETS_FETCHED_LIST,new BasicDBObject("$each",topic.get(Constants.TOPIC_TWEETS_FETCHED_LIST)));
			topic.remove(Constants.TOPIC_TWEETS_FETCHED_LIST);
			collection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
		}
	}
	
	/**
	 * delete a topic from DB
	 * 
	 * @param topicId
	 * @author qiumm
	 */
	public void deleteTopic(String topicId) {
		if (collection == null) {
		collection = this.getDBCollection(TopicCollection);
	}
		if (StringUtils.isEmpty(topicId)) {
			throw new RuntimeException("the property of tweetsId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, topicId);
		collection.findAndRemove(query);
	}

	/**
	 * query topic info
	 * @param userId
	 */
	public Map<?, ?> queryOne(Long topicId) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		Map<?, ?> result = null;
		if (topicId == null) {
			throw new RuntimeException("the property of topicId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, topicId);
		DBObject one = collection.findOne(query);
		if(one != null){
			result = one.toMap();
		}
		return result;
	}
	
	public Map<?, ?> queryOne(String topicId) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		Map<?, ?> result = null;
		if (topicId == null) {
			throw new RuntimeException("the property of topicId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, topicId);
		DBObject one = collection.findOne(query);
		if(one != null){
			result = one.toMap();
		}
		return result;
	}
	
	/**
	 * query topic info
	 * @param topicId
	 */
	public Map<?, ?> queryOne(UUID topicId) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		Map<?, ?> result = null;
		if (topicId == null) {
			throw new RuntimeException("the property of topicId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, topicId);
		DBObject one = collection.findOne(query);
		if(one != null){
			result = one.toMap();
		}
		return result;
	}
	
	/**
	 * query topic info
	 * @param topicName
	 */
	public Map<?, ?> queryOneByName(String topicName) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		Map<?, ?> result = null;
		if (topicName == null) {
			throw new RuntimeException("the property of topicName is empty!");
		}
		BasicDBObject query = new BasicDBObject(Constants.TOPIC_NAME, topicName);
		DBObject one = collection.findOne(query);
		if(one != null){
			result = one.toMap();
		}
		return result;
	}
	

	/**
	 * query started topic info
	 */
	public List<DBObject> query() {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		List<DBObject> result = new ArrayList<DBObject>();
		BasicDBObject query = new BasicDBObject(Constants.TOPIC_STATE, Constants.TOPIC_STATE_START);
		DBCursor find = collection.find(query);
		if(find != null){
			result = find.toArray();
		}
		find.close();
		return result;
	}
	

	/**
	 * query topic info
	 */
	public List<DBObject> query(int skip,int count) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		DBObject keys = new BasicDBObject(Constants.TOPIC_URL,Constants.TOPIC_URL);
		DBObject query = new BasicDBObject();
		DBObject order = new BasicDBObject("$natural","1");
		DBCursor cursor = collection.find(query,keys).sort(order).skip(skip).limit(count);
		List<DBObject> list = cursor.toArray();
		cursor.close();
		return list;
	}

	/**
	 * query started topic info
	 */
	public List<DBObject> query(int days) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		List<DBObject> result = new ArrayList<DBObject>();
		BasicDBObject query = new BasicDBObject();
		Date startTime=new Date();
		startTime = DateTimeUtils.add(startTime, Calendar.DAY_OF_MONTH, (-1)*days);
		query.put(Constants.FETCH_TIME, new BasicDBObject().append("$gt", startTime));  
//		query.put(Constants.TOPIC_STATE,status);
		DBCursor find = collection.find(query).sort(new BasicDBObject(Constants.FETCH_TIME,-1));
		if(find != null){
			result = find.toArray();
		}
		find.close();
		return result;
	}
}
