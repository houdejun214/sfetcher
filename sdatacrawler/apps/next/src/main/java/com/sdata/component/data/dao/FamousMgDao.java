package com.sdata.component.data.dao;

import java.util.Date;
import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Repository;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.data.FieldProcess;

/**
 * save user data to mongodb database
 * 
 * @author zhufb
 * 
 */
@Repository
public class FamousMgDao extends MongoDao {
	private static final String ID = Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String starCollection = "famous";
	private static final String tweetList = "tl";
	private DBCollection twCollection;

	/**
	 * save a star to DB
	 * @param user
	 */
	public void save(Map<String, Object> star,FieldProcess fieldProcess) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(starCollection);
		}
		Object id = star.get(ID);
		if(id ==null ){
			id = star.get(Constants.FAMOUS_ID);
			if(id==null)
				throw new RuntimeException("the property of userid is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		if(star.containsKey(tweetList)){
			BasicDBObject update = new BasicDBObject();
			update.put(tweetList,new BasicDBObject("$each",star.get(tweetList)));
			star.remove(tweetList);
			twCollection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
		}
		if(star.containsKey(Constants.FAMOUS_TWEETS_LIST)){
			BasicDBObject update = new BasicDBObject();
			update.put(Constants.FAMOUS_TWEETS_LIST,new BasicDBObject("$each",star.get(Constants.FAMOUS_TWEETS_LIST)));
			star.remove(Constants.FAMOUS_TWEETS_LIST);
			twCollection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
		}
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(star);
		doc.put(Constants.FETCH_TIME, new Date());
		doc.remove(ID);
		twCollection.findAndModify(query, null, null, false, new BasicDBObject("$set",doc), false,true);
		doc.put(ID, id);
		if(id instanceof UUID){
			doc.put(ID, UUIDUtils.encode((UUID)id));
		}
		doc.put("origin", CrawlAppContext.conf.get(Constants.SOURCE));
		fieldProcess.solrIndex(doc);
	}

	/**
	 * delete a star from DB
	 * @param userId
	 */
	public void delete(String starId) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(starCollection);
		}
		if (StringUtils.isEmpty(starId)) {
			throw new RuntimeException("the property of userId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, starId);
		twCollection.findAndRemove(query);
	}
	

	/**
	 * query user's newest tweet id
	 * @param userId
	 */
	public Long querySinceId(Long starId) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(starCollection);
		}
		Long result = 0L;
		if (starId == null) {
			throw new RuntimeException("the property of userId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID, starId);
		DBObject one = twCollection.findOne(query);
		if(one!=null&&one.containsField(Constants.FAMOUS_NEWEST_TWEET)){
			result = (Long)one.get(Constants.FAMOUS_NEWEST_TWEET);
		}
		return result;
	}
	
	/**
	 * query user's newest tweet id
	 * @param userId
	 */
	public DBObject queryByName(String name) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(starCollection);
		}
		if (name == null) {
			throw new RuntimeException("the property of userId is empty!");
		}
		BasicDBObject query = new BasicDBObject(Constants.FAMOUS_NAME, name);
		DBObject result = twCollection.findOne(query);
		return result;
	}

}
