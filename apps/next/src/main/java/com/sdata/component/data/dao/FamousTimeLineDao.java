package com.sdata.component.data.dao;

import java.util.Date;
import java.util.Map;

import org.springframework.stereotype.Repository;

import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;

/**
 * save user data to mongodb database
 * 
 * @author zhufb
 * 
 */
@Repository
public class FamousTimeLineDao extends MongoDao {
	private static final String ID = Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String starCollection = "famous.timeline";
	private DBCollection twCollection;

	/**
	 * save a star to DB
	 * @param user
	 */
	public void save(Map<String, Object> star) {
		if (twCollection == null) {
			twCollection = this.getDBCollection(starCollection);
		}
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(star);
		doc.put(Constants.FETCH_TIME, new Date());
		twCollection.save(doc);
	}

}
