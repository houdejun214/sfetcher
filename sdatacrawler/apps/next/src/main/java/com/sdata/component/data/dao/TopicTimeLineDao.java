package com.sdata.component.data.dao;

import java.util.Map;

import org.springframework.stereotype.Repository;

import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;

/**
 * save topic's timeline data to  database
 * 
 * @author zhufb
 *
 */
@Repository
public class TopicTimeLineDao extends MongoDao {
	private static final String TopicCollection = "topics.timeline";
	private DBCollection collection;

	/**
	 * save a new topic timeline to DB
	 * 
	 * @param topic
	 */
	public void save(Map<String, Object> topic) {
		if (collection == null) {
			collection = this.getDBCollection(TopicCollection);
		}
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(topic);
		collection.save(doc);
	}
}
