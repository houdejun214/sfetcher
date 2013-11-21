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
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.sdata.core.Constants;

/**
 * save user data to mongodb database
 * 
 * @author zhufb
 * 
 */
@Repository
public class UserMgDao extends MongoDao {
	
	private static final String ID = Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String userCollection = "users";
	private static final BasicDBObject FIELDS_EMPTY=new BasicDBObject("_id",1);
	private DBCollection twCollection;

	/**
	 * save a user to DB
	 * @param user
	 */
	public void saveUser(Map<String, Object> user) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		Object id = user.get(ID);
		if(id ==null ){
			Object uid = user.get(Constants.USER_ID);
			if(uid == null){
				throw new RuntimeException("the property of userid is empty!");
			}
			id = uid;
			user.put(ID, id);
		}else if(!(id instanceof Long || id instanceof UUID)){
			String uid = StringUtils.valueOf(id);
			if("".equals(uid))
				throw new RuntimeException("the property of userid is empty!");
			id = Long.valueOf(uid);
			user.put(ID, id);
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(user);
		twCollection.findAndModify(query, null, null, false, doc, false, true );
	}
	
	/**
	 * save a user to DB
	 * @param user
	 */
	public void updateNewTweetsDate(Object uid,Date date) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		if(null ==uid ){
			throw new RuntimeException("the property of userid is empty!");
		}
		BasicDBObject query = new BasicDBObject();
		query.put(ID,uid);
		BasicDBObject doc = new BasicDBObject("$set",new BasicDBObject(Constants.FAMOUS_NEWEST_TWEET_DATE,date));
		twCollection.update(query, doc, true, true );
	}
	
	/**
	 * save a user to DB
	 * @param user
	 */
	public boolean insertUser(Map<String, Object> user) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		Object id = user.get(ID);
		if(id ==null ){
			throw new RuntimeException("the property of tweetsId is empty!");
		}else if(!(id instanceof Long || id instanceof UUID)){
			String tweetsId = StringUtils.valueOf(id);
			if("".equals(tweetsId))
				throw new RuntimeException("the property of tweetsId is empty!");
			id = Long.valueOf(tweetsId);
			user.put(ID, id);
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		if(twCollection.count(query)==0){
			BasicDBObject doc = new BasicDBObject();
			doc.putAll(user);
			twCollection.insert(doc);
			return true;
		}
		return false;
	}

	/**
	 * delete a user from DB
	 * @param userId
	 */
	public void deleteUser(Long userId) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		if (userId == null) {
			throw new RuntimeException("the property of userId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, userId);
		twCollection.findAndRemove(query);
	}
	
	/**
	 * query a user from DB
	 * @param user
	 */
	public DBObject query(Long userId) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		if (userId == null) {
			throw new RuntimeException("the property of userId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, userId);
		DBObject one = twCollection.findOne(query);
		return one;
	}
	
	/**
	 * query a user from DB
	 * @param user
	 */
	public DBObject query(UUID userId) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		if (userId == null) {
			throw new RuntimeException("the property of userId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, userId);
		DBObject one = twCollection.findOne(query);
		return one;
	}
	
	/**
	 * query a user from DB
	 * @param user
	 */
	public DBObject query(String screenName) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		if (screenName == null) {
			throw new RuntimeException("the property of screenName is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, screenName);
		DBObject one = twCollection.findOne(query);
		return one;
	}
	
	/**
	 * check if a user is exists
	 * 
	 * @param userId
	 * @return
	 */
	public boolean isExists(Object userId){
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		if (userId == null) {
			throw new RuntimeException("the property of userId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, userId);
		Object result = twCollection.findOne(query,FIELDS_EMPTY);
		return result!=null;
	}

	public List<String> getNextFetchList(int lastCount,int topN){
		List<String> fetchList = new ArrayList<String>();
		if (twCollection == null) {
			twCollection = this.getDBCollection(userCollection);
		}
		DBObject orderBy = new BasicDBObject();
		orderBy.put("_id","1");
		DBCursor cursor = twCollection.find().sort(orderBy).skip(lastCount).limit(topN);
		List<DBObject> cursorList = cursor.toArray();
		for(DBObject queryObj:cursorList){
			String id = StringUtils.valueOf(queryObj.get("nsid"));
			fetchList.add(id);
		}
		return fetchList;
	}
}
