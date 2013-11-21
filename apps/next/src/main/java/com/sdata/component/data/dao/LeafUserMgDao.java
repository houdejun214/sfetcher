package com.sdata.component.data.dao;

import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Repository;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.sdata.core.Constants;

/**
 * save user data to mongodb database
 * 
 * @author zhufb
 * 
 */
@Repository
public class LeafUserMgDao extends MongoDao {
	private static final String ID = Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String userCollection = "leafusers";
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
			throw new RuntimeException("the property of userId is empty!");
		}else if(!(id instanceof Long || id instanceof UUID)){
			String tweetsId = StringUtils.valueOf(id);
			if("".equals(tweetsId))
				throw new RuntimeException("the property of userId is empty!");
			id = Long.valueOf(tweetsId);
			user.put(ID, id);
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(user);
		twCollection.findAndModify(query, null, null, false, doc, false, true );
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
		long count = twCollection.count(query);
		return count>0;
	}

}
