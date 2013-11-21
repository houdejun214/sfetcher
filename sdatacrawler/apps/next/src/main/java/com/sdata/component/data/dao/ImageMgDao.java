package com.sdata.component.data.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.nus.next.db.NextSourceDao;
import com.nus.next.db.NextSourceDbManager;
import com.sdata.core.Constants;
import com.sdata.core.data.FieldProcess;

/**
 * @author houdj
 *
 */
public class ImageMgDao{
	
	private static final String ImagesCollection="images";
	private static final String commentsCollectionName="comments";
	
	
	private DBCollection imgCollecion;
	private DBCollection commentsCollection;
	//private String core;
	private NextSourceDao imgDao;
	
	public ImageMgDao(String sourceName){
		imgDao = NextSourceDbManager.getSourceDao(sourceName);
	}
	
	public boolean saveImage(Map<String, Object> image,FieldProcess columMap){
		if(imgCollecion==null){
			imgCollecion =imgDao.getDBCollection(ImagesCollection);
		}
		if(commentsCollection==null){
			commentsCollection = imgDao.getDBCollection(commentsCollectionName);
		}
		Object id = image.get("orgid");
		if(id == null ){
			throw new RuntimeException("the property of imageId is empty!");
		}
		if(!(id instanceof Long || id instanceof UUID)){
			String imageId = StringUtils.valueOf(id);
			if("".equals(imageId)) throw new RuntimeException("the property of imageId is empty!");
			id = Long.valueOf(imageId);
			image.put(Constants.OBJECT_ID, Long.valueOf(imageId));
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,(id instanceof UUID)?id:(Long)id);
		
		if(image.containsKey(Constants.COMTS)){
			boolean isCommentsIndependence = false;
			if(image.containsKey(Constants.FLICKR_IS_COMMENTS_INDEPENDENCE)){
				isCommentsIndependence = (Boolean)image.get(Constants.FLICKR_IS_COMMENTS_INDEPENDENCE);
			}
			if(isCommentsIndependence){
				storeCommentsIndependence(image, query);
			}else{
				BasicDBObject update = new BasicDBObject();
				update.put(Constants.COMTS,new BasicDBObject("$each",image.get(Constants.COMTS)));
				image.remove(Constants.COMTS);
				imgCollecion.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
				DBObject find = imgCollecion.findOne(query);
				if(find!=null&&find.containsField(Constants.COMTS)){
					List reviews = (List)find.get(Constants.COMTS);
					if(reviews!=null&&reviews.size() > 0)
						image.put(Constants.REVIEWS_NUM, reviews.size());
				}
			}
			
		}
		
		BasicDBObject doc = new BasicDBObject();
		image.remove(Constants.OBJECT_ID);
		doc.putAll(image);
		imgCollecion.findAndModify( query, null, null, false, new BasicDBObject("$set",doc), false, true );
		
		image.put(Constants.OBJECT_ID, (id instanceof UUID)?UUIDUtils.encode((UUID)id):(Long)id);
		columMap.solrIndex(image);
		return true;
	}

	private void storeCommentsIndependence(Map<String, Object> image,
			BasicDBObject query) {
		List comts = (ArrayList)image.get(Constants.COMTS);
		List<DBObject> comtsList = new ArrayList<DBObject>();
		List<Long> comtsIdList = new ArrayList<Long>();
		for(int i=0;i<comts.size();i++){
			Long nextSeqId = imgDao.getNextSeqId(commentsCollectionName);
			Map<String,Object> comt = (Map<String,Object>)comts.get(i);
			comt.put("imageId", image.get(Constants.OBJECT_ID));
			comt.put("_id", nextSeqId);
			DBObject comtObj = new BasicDBObject();
			comtObj.putAll(comt);
			comtsList.add(comtObj);
			comtsIdList.add(nextSeqId);
		}
		commentsCollection.insert(comtsList);
		BasicDBObject update = new BasicDBObject();
		update.put(Constants.COMTS_ID,new BasicDBObject("$each",comtsIdList));
		image.remove(Constants.COMTS);
		imgCollecion.findAndModify(query, null, null, false, new BasicDBObject("$addToSet",update), false, true);
		DBObject find = imgCollecion.findOne(query);
		if(find!=null&&find.containsField(Constants.COMTS_ID)){
			List reviews = (List)find.get(Constants.COMTS_ID);
			if(reviews!=null&&reviews.size() > 0)
				image.put(Constants.REVIEWS_NUM, reviews.size());
		}
	}
	
	public void deleteImage(Long imageId) {
		if(imageId==null){
			return;
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,imageId);
		if(imgCollecion==null){
			imgCollecion = imgDao.getDBCollection(ImagesCollection);
		}
		imgCollecion.findAndRemove(query);
	}
	
	/**
	 * check whether the image is exists
	 * @param tweetsId
	 * @return
	 */
	public boolean isImageExists(String imageId){
		if(StringUtils.isEmpty(imageId)){
			throw new RuntimeException("imageId Id is empty");
		}
		Object shardKey = imgDao.generateShardKey(imageId);
		Object _id = imgDao.getUUID(imageId);
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,_id);
		query.append("shkey", shardKey);
		if(imgCollecion==null){
			imgCollecion = imgDao.getDBCollection(ImagesCollection);
		}
		boolean hasNext = imgCollecion.findOne(query)!=null;
		return hasNext;
	}
	
	public List<Map<String,Object>> getNextFetchList(String collectionName,int lastCount,int topN){
		List<Map<String,Object>> fetchList = new ArrayList<Map<String,Object>>();
		if (imgCollecion == null) {
			imgCollecion = imgDao.getDBCollection(collectionName);
		}
		DBObject orderBy = new BasicDBObject();
		orderBy.put("$natural","1");
		BasicDBObject fieldsOnly=new BasicDBObject("_id",1);
		fieldsOnly.put("orgid", 1);
		fieldsOnly.put("imgur", 1);
		fieldsOnly.put("ownid", 1);
		DBCursor cursor = imgCollecion.find(null,fieldsOnly).sort(orderBy).skip(lastCount).limit(topN);
		List<DBObject> cursorList = cursor.toArray();
		for(DBObject queryObj:cursorList){
			Map<String,Object> fetchMap = new HashMap<String,Object>();
			fetchMap = (Map<String,Object>)queryObj.toMap();
			fetchList.add(fetchMap);
		}
		return fetchList;
	}
	
	
	public List<Map<String,Object>> getNextFetchList(int lastCount,int topN){
		return this.getNextFetchList(ImagesCollection, lastCount, topN);
	}
}
