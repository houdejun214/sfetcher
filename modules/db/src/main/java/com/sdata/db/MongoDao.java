package com.sdata.db;

import java.util.Iterator;
import java.util.Map;

import com.framework.db.mongo.NextSourceDao;
import com.lakeside.core.utils.StringUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.sdata.context.config.Constants;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.extension.statistic.Statistic;

/**
 * @author zhufb
 *
 */
public class MongoDao implements BaseDao {
	private DaoCollection storeCollection ;
	private DBCollection dbCollection;
	private NextSourceDao sourceDao;
	private Statistic statisStore;
	
	public MongoDao(NextSourceDao sourceDao,DaoCollection storeCollection){
		this.storeCollection = storeCollection;
		this.sourceDao = sourceDao;
		this.statisStore = new Statistic(CrawlAppContext.conf);
		this.dbCollection = sourceDao.getDBCollection(storeCollection.getName());
	}

	public void save(Map<String,Object> data){
		Object objectId = data.remove(Constants.OBJECT_ID);
		if(objectId == null){
			if(StringUtils.isEmpty(storeCollection.getPrimaryKey())){
				throw new RuntimeException("the property _id of object id is empty and no referce primaryKey!");
			}
			objectId = data.get(storeCollection.getPrimaryKey());
		}

		BasicDBObject doc = new BasicDBObject();
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,objectId);
		Object shkey = sourceDao.generateShardKey(storeCollection.getName(), data);
		if(shkey != null&&!"".equals(shkey)){
			query.append(Constants.MONGO_SHKEY,shkey);
		}
		if(objectId != null){
			query.append(Constants.OBJECT_ID, objectId);
			// update field deal
			this.updateFields(query,data);
		}
		
		// remove field deal
		this.removeFields(data);
		
		doc.putAll(data);
		if(objectId == null){
			dbCollection.save(doc);
			return;
		}
		
		// save other info
		DBObject dbo = dbCollection.findAndModify(query, null, null, false, new BasicDBObject("$set",doc), false, true );
		
		// statistic
		if(dbo==null){
			statisStore.increase(sourceDao.getSourceName(),storeCollection.getName());
		}
	}
	private void updateFields(BasicDBObject query,Map<String,Object> data){
		Iterator<String> iterator = storeCollection.getUpdate().iterator();
		while(iterator.hasNext()){
			String field = iterator.next();
			BasicDBObject update = new BasicDBObject();
			if(data.containsKey(field)){
				update.put(field,new BasicDBObject("$each",data.get(field)));
				data.remove(field);
				dbCollection.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
			}
		}
	}

	private void removeFields(Map<String,Object> data){
		Iterator<String> iterator = storeCollection.getRemove().iterator();
		while(iterator.hasNext()){
			String field = iterator.next();
			data.remove(field);
		}
	}
	
	/**
	 * select count until now
	 * 
	 * @param count
	 * @return
	 */
	public boolean isExists(Object id){
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		DBObject one = dbCollection.findOne(query,new BasicDBObject(Constants.OBJECT_ID,1));
		if(one == null){
			return false;
		}
		return true;
	}
	
	/**
	 * select count until now
	 * 
	 * @param count
	 * @return
	 */
	public void delete(Object id){
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		dbCollection.findAndRemove(query);
	}
}
