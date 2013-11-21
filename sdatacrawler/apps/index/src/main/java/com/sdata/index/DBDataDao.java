package com.sdata.index;

import java.util.Date;
import java.util.List;
import java.util.Map;

import com.lakeside.core.utils.time.DateFormat;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.sdata.core.Constants;

/**
 * @author zhufb
 *
 */
public class DBDataDao extends MongoDao{
	private String collection ;
	private DBCollection dbCollection;
	public DBDataDao(String collection){
		this.collection = collection;
	}
	/**
	 * select count and skip some elements until time
	 * 
	 * @param count
	 * @param skip
	 * @param time
	 * @return
	 */
	public List<DBObject> query(int count,long maxId){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		DBObject keys = new BasicDBObject(Constants.OBJECT_ID,Constants.OBJECT_ID);
		DBObject query = new BasicDBObject(Constants.OBJECT_ID, new BasicDBObject("$gt",maxId));
		DBObject order = new BasicDBObject("_id","1");
		DBCursor cursor = dbCollection.find(query,keys).sort(order).limit(count);
		List<DBObject> list = cursor.toArray();
		cursor.close();
		return list;
	}
	
	/**
	 * select count which gt fetchTime limit count 
	 * 
	 * @param count
	 * @param maxFetchTime
	 * @return
	 */
	public List<DBObject> query(int count,Date maxFetchTime){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		DBObject keys = new BasicDBObject(Constants.OBJECT_ID,Constants.OBJECT_ID).append(Constants.FETCH_TIME, Constants.FETCH_TIME);
		DBObject query = new BasicDBObject(Constants.FETCH_TIME, new BasicDBObject("$gt",maxFetchTime));
		DBObject order = new BasicDBObject(Constants.FETCH_TIME,"1");
		DBCursor cursor = dbCollection.find(query,keys).sort(order).limit(count);
		List<DBObject> list = cursor.toArray();
		cursor.close();
		return list;
	}
	
	public List<DBObject> query(int count,int skip,Date time){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		if(time == null){
			time = new Date();
		}
		DBObject keys = new BasicDBObject(Constants.OBJECT_ID,Constants.OBJECT_ID);
		DBObject query = new BasicDBObject();
		DBObject order = new BasicDBObject("$natural","1");
		DBCursor cursor = dbCollection.find(query,keys).sort(order).skip(skip).limit(count);
		List<DBObject> list = cursor.toArray();
		cursor.close();
		return list;
	}
	
	@SuppressWarnings("unchecked")
	public List query(String column,Date time){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		DBObject query = new BasicDBObject(Constants.FETCH_TIME, BasicDBObjectBuilder.start("$gte", time).get());
		return dbCollection.distinct(column, query);
	}
	
	public List<DBObject> query(int count){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		DBObject keys = new BasicDBObject(Constants.OBJECT_ID,Constants.OBJECT_ID);
		Integer [] a = {10,1};
		DBObject query = new BasicDBObject(Constants.OBJECT_ID,new BasicDBObject("$mod",a));
		DBObject order = new BasicDBObject("$natural","1");
		DBCursor cursor = dbCollection.find(query,keys).sort(order).limit(count);
		List<DBObject> list = cursor.toArray();
		cursor.close();
		return list;
	}

	public List<DBObject> query(int type,int count){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		DBObject keys = new BasicDBObject(Constants.OBJECT_ID,Constants.OBJECT_ID);
		DBObject query = new BasicDBObject(Constants.OBJECT_ID,new BasicDBObject("$type",type));
		DBObject order = new BasicDBObject("$natural","1");
		DBCursor cursor = dbCollection.find(query,keys).sort(order).limit(count);
		List<DBObject> list = cursor.toArray();
		cursor.close();
		return list;
	}
	
	/**
	 * select count until now
	 * 
	 * @param count
	 * @return
	 */
	public DBObject query(Object id){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		return dbCollection.findOne(query);
	}

	/**
	 * select count until now
	 * 
	 * @param count
	 * @return
	 */
	public List<DBObject> query(String key,Object value){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		BasicDBObject query = new BasicDBObject(key,value);
		DBCursor find = dbCollection.find(query);
		List<DBObject> array = find.toArray();
		find.close();
		return array;
	}
	
	/**
	 * select count until now
	 * 
	 * @param count
	 * @return
	 */
	public List<DBObject> query(Long[] ids) {
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,new BasicDBObject("$in", ids));
		DBObject keys = new BasicDBObject("text", "content");
		DBCursor cursor = dbCollection.find(query, keys);
		List<DBObject> list = cursor.toArray();
		cursor.close();
		return list;
	}
	
	/**
	 * select count until now
	 * 
	 * @param count
	 * @return
	 */
	public boolean isExists(Object id){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
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
	public void update(Map<String,Object> data){
		Object id = data.get(Constants.OBJECT_ID);
		if(!data.containsKey(Constants.FETCH_TIME)){
			data.put(Constants.FETCH_TIME, new Date());
		}
		Object fdate = DateFormat.changeStrToDate(data.get(Constants.FETCH_TIME));
		data.put(Constants.FETCH_TIME, fdate);
		if(data.containsKey(Constants.CREATE_TIME)){
			Object cdate = DateFormat.changeStrToDate(data.get(Constants.CREATE_TIME));
			data.put(Constants.CREATE_TIME, cdate);
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(data);
		dbCollection.findAndModify(query, null, null, false, doc, false, true);
			
	}
	
	/**
	 * select count until now
	 * 
	 * @param count
	 * @return
	 */
	public DBObject delete(Object id){
		if(dbCollection == null){
			dbCollection = this.getDBCollection(collection);
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		return dbCollection.findAndRemove(query);
	}
}
