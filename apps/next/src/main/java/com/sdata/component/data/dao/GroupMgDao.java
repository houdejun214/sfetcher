package com.sdata.component.data.dao;

import java.util.ArrayList;
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
public class GroupMgDao extends MongoDao {
	private static final String ID = Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String groupCollection = "groups";
	private DBCollection twCollection;

	/**
	 * save a group to DB
	 * @param group
	 */
	public void saveGroup(Map<String, Object> user) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(groupCollection);
		}
		Object id = user.get(ID);
		if(id ==null ){
			throw new RuntimeException("the property of groupId is empty!");
		}else if(!(id instanceof Long || id instanceof UUID)){
			String groupId = StringUtils.valueOf(id);
			if("".equals(groupId))
				throw new RuntimeException("the property of groupId is empty!");
			id = Long.valueOf(groupId);
			user.put(ID, id);
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(user);
		twCollection.findAndModify(query, null, null, false, doc, false, true );
	}

	/**
	 * delete a group from DB
	 * @param groupId
	 */
	public void deleteGroup(Long groupId) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(groupCollection);
		}
		if (groupId == null) {
			throw new RuntimeException("the property of groupId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, groupId);
		twCollection.findAndRemove(query);
	}
	
	/**
	 * query a group from DB
	 * @param group
	 */
	public DBObject query(Long groupId) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(groupCollection);
		}
		if (groupId == null) {
			throw new RuntimeException("the property of groupId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, groupId);
		DBObject one = twCollection.findOne(query);
		return one;
	}
	
	
	/**
	 * check if a group is exists
	 * 
	 * @param groupId
	 * @return
	 */
	public boolean isExists(Object groupId){
		if (twCollection == null) {
			twCollection = this.getDBCollection(groupCollection);
		}
		if (groupId == null) {
			throw new RuntimeException("the property of groupId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, groupId);
		long count = twCollection.count(query);
		return count>0;
	}
	
	public List<String> getNextFetchList(int lastCount,int topN){
		List<String> fetchList = new ArrayList<String>();
		if (twCollection == null) {
			twCollection = this.getDBCollection(groupCollection);
		}
		DBObject orderBy = new BasicDBObject();
		orderBy.put("$natural","-1");
		DBCursor cursor = twCollection.find().sort(orderBy).skip(lastCount).limit(topN);
		List<DBObject> cursorList = cursor.toArray();
		for(DBObject queryObj:cursorList){
			String id = StringUtils.valueOf(queryObj.get("id"));
			fetchList.add(id);
		}
		cursor.close();
		return fetchList;
	}

}
