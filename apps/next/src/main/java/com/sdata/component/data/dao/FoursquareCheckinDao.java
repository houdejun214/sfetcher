package com.sdata.component.data.dao;

import java.util.Map;

import org.bson.types.ObjectId;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;
import com.sdata.core.data.FieldProcess;

/**
 * foursquare checkin date access object
 * 
 * @author houdejun
 *
 */
public class FoursquareCheckinDao  extends MongoDao{

	private DBCollection checkinCollecion;
	private DBCollection userCollecion;
	private DBCollection venueCollecion;
	private static final String CHECKIN_NAME = "checkins";
	private static final String USER_NAME = "users";
	private static final String VENUE_NAME = "venues";
	private static final String CHECKIN_ID="chkid";
	///private String core;
	
	@Override
	public void initilize(String host, int port, String dbName) {
		//core = dbName.concat(".").concat(CHECKIN_NAME);
		super.initilize(host, port, dbName);
	}
	/**
	 * save a checkin date to db
	 * @param columMap 
	 * @param user
	 */
	public void saveCheckin(Map<String, Object> meta, FieldProcess columMap) {
		if (checkinCollecion == null) {
			checkinCollecion = this.getDBCollection(CHECKIN_NAME);
		}
		Map<String, Object> user = (Map<String, Object>) meta.remove("user");
		if(user!=null){
			Object userId = saveUser(user);
			meta.put("usrid",userId);
		}
		
		Object _venue = meta.remove("venue");
		if(_venue!=null && _venue instanceof Map){
			Map<String, Object> venue = (Map<String, Object>) _venue;
			String id = StringUtils.valueOf(venue.get("id"));
			String name = StringUtils.valueOf(venue.get("name"));
			venue.put("id", StringUtils.chompHeader(id, "v"));
			Object venueId = saveVenue(venue);
			meta.put("vid",venueId);
			meta.put("lng",getEmbedMapValue(venue,"loc","lng"));
			meta.put("lat",getEmbedMapValue(venue,"loc","lat"));
			meta.put("vname",name);
		}
		
		Object id = ensureObjectID(meta,CHECKIN_ID,"id");
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(meta);
		checkinCollecion.findAndModify(query, null, null, false, doc, false, true );
		//index
		columMap.solrIndex(meta);
	}
	
	private Object getEmbedMapValue(Map<String,?> map,String firstKey,String secKey){
		if(map==null){
			return null;
		}
		if(map.containsKey(firstKey)){
			map = (Map<String,?>)map.get(firstKey);
			if(map!=null){
				return map.get(secKey);
			}
		}
		return null;
	}
	
	/**
	 * save venue
	 * 
	 * @param meta
	 * @return
	 */
	public Object saveVenue(Map<String, Object> meta){
		if (venueCollecion == null) {
			venueCollecion = this.getDBCollection(VENUE_NAME);
		}
		Object venueId = ensureObjectID(meta);
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,venueId);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(meta);
		//venueCollecion.save(doc);
		venueCollecion.findAndModify(query, null, null, false, doc, false, true );
		return venueId;
	}
	
	/**
	 * save user
	 * 
	 * @param meta
	 * @return
	 */
	public Object saveUser(Map<String, Object> meta){
		if (userCollecion == null) {
			userCollecion = this.getDBCollection(USER_NAME);
		}
		Object userId = ensureObjectID(meta);
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,userId);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(meta);
		//userCollecion.save(doc);
		userCollecion.findAndModify(query, null, null, false, doc, false, true );
		return userId;
	}
	
	private Object ensureObjectID(Map<String, Object> meta){
		return this.ensureObjectID(meta, "id");
	}
	
	public Boolean isCheckinExists(Object id){
		if(id==null){
			throw new RuntimeException("checkIn Id is empty");
		}
		id = getObjectId(id);
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		if (checkinCollecion == null) {
			checkinCollecion = this.getDBCollection(CHECKIN_NAME);
		}
		boolean hasNext = checkinCollecion.find(query).hasNext();
		return hasNext;
	}
	
	private Object ensureObjectID(Map<String, Object> meta,String key){
		Object id = meta.get(Constants.OBJECT_ID);
		if(id ==null ){
			id = meta.get(key);
			id = getObjectId(id);
			meta.put(Constants.OBJECT_ID, id);
			return id;
		}
		return id;
	}
	
	private Object ensureObjectID(Map<String, Object> meta,String key,String key1){
		Object id = meta.get(Constants.OBJECT_ID);
		if(id ==null ){
			id = meta.get(key);
			if(id == null){
				id = meta.get(key1);
			}
			id = getObjectId(id);
			meta.put(Constants.OBJECT_ID, id);
			return id;
		}
		return id;
	}

	private Object getObjectId(Object id) {
		if(id ==null ){
			throw new RuntimeException("the property of id is null!");
		}
		String strId = StringUtils.valueOf(id);
		if("".equals(strId))
			throw new RuntimeException("the property of id is empty!");
		if(ObjectId.isValid(strId)){
			id = new ObjectId(strId);
		}else if(StringUtils.isNum(strId)){
			id = Long.valueOf(strId);
		}else {
			throw new RuntimeException("the property of id is a invalid string");
		}
		return id;
	}
}
