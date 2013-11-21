package com.sdata.component.data.dao;

import java.util.Map;

import org.springframework.stereotype.Repository;

import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;

/**
 * save user data to mongodb database
 * 
 * @author qmm
 * 
 */
@Repository
public class ReviewsMgDao extends MongoDao {
	
	private static final String ID = Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String collection = "reviews";
	private static final BasicDBObject FIELDS_EMPTY=new BasicDBObject("_id",1);
	private DBCollection twCollection;

	/**
	 * save a comment to DB
	 * @param review
	 */
	public void save(Map<String, Object> review) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(collection);
		}
		Object id = review.get(ID);
		if(id ==null ){
			throw new RuntimeException("the property of commentId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(review);
		twCollection.findAndModify(query, null, null, false, doc, false, true );
	}
	
	/**
	 * save a comment to DB
	 * @param review
	 */
	public boolean insert(Map<String, Object> review) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(collection);
		}
		Object id = review.get(ID);
		if(id ==null ){
			throw new RuntimeException("the property of tweetsId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		if(twCollection.count(query) == 0){
			BasicDBObject doc = new BasicDBObject();
			doc.putAll(review);
			twCollection.insert(doc);
			return true;
		}
		return false;
	}
	
	/**
	 * check if a comment is exists
	 * 
	 * @param reviewId
	 * @return
	 */
	public boolean isExists(Object reviewId){
		if (twCollection == null) {
			twCollection = this.getDBCollection(collection);
		}
		if (reviewId == null) {
			throw new RuntimeException("the property of userId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, reviewId);
		Object result = twCollection.findOne(query,FIELDS_EMPTY);
		return result!=null;
	}
}
