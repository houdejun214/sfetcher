package com.sdata.component.data.dao;

import java.util.Map;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;

/**
 * @author houdj
 *
 */
public class FavoritesMgDao extends MongoDao {
	
	private static final String FavoritesCollectionName="favorites";
	
	private DBCollection favCollecion;
	//private String core;
	
	@Override
	public void initilize(String host, int port, String dbName) {
	//	core = dbName.concat(".").concat(ImagesCollection);
		super.initilize(host, port, dbName);
	}
	
	public boolean saveFavorite(Map<String, Object> favorite){
		if(favCollecion==null){
			favCollecion = getDBCollection(FavoritesCollectionName);
		}
		
		Long id = (Long)favorite.get(Constants.OBJECT_ID);
		if(id == null ){
			String imageId = StringUtils.valueOf(favorite.get("orgid"));
			if("".equals(imageId)) throw new RuntimeException("the property of imageId is empty!");
			id = Long.valueOf(imageId);
			favorite.put(Constants.OBJECT_ID, Long.valueOf(imageId));
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		
		BasicDBObject doc = new BasicDBObject();
		favorite.remove(Constants.OBJECT_ID);
		doc.putAll(favorite);
		favCollecion.findAndModify( query, null, null, false, new BasicDBObject("$set",doc), false, true );
		
		return true;
	}
	
	
	/**
	 * check whether the image is exists
	 * @param tweetsId
	 * @return
	 */
	public boolean isFavoriteExists(Long favId){
		if(favId==null){
			throw new RuntimeException("favId Id is empty");
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,favId);
		if(favCollecion==null){
			favCollecion = this.getDBCollection(FavoritesCollectionName);
		}
		boolean hasNext = favCollecion.find(query).hasNext();
		return hasNext;
	}
}
