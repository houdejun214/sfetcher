package com.sdata.component.data.dao;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.lakeside.core.utils.UUIDUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;
import com.sdata.core.data.FieldProcess;

/**
 * @author houdj
 *
 */
public class VideoMgDao extends MongoDao {
	
	private static final String VideoCollection="videos";
	
	private DBCollection videoCollecion;
	//private String core;
	
	private UserMgDao userdao = new UserMgDao();
	private CommentsMgDao comtsdao = new CommentsMgDao();
	
	@Override
	public void initilize(String host, int port, String dbName) {
		super.initilize(host, port, dbName);
		userdao.initilize(this.m, this.db);
		comtsdao.initilize(this.m, this.db);
	}
	
	public boolean saveVideo(Map<String, Object> video,FieldProcess columMap){
		Object id = video.get(Constants.OBJECT_ID);
		if(id == null ){
			throw new RuntimeException("the property of Id is empty!");
		}
		if(videoCollecion==null){
			videoCollecion = getDBCollection(VideoCollection);
		}
		//save video user
		this.saveUser(video);
		//save comments
		this.saveComts(video);
		//save video
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		BasicDBObject doc = new BasicDBObject();
		video.remove(Constants.OBJECT_ID);
		doc.putAll(video);
		videoCollecion.findAndModify( query, null, null, false, new BasicDBObject("$set",doc), false, true );
		
		//index
		video.put(Constants.OBJECT_ID, id);
		columMap.solrIndex(video);
		return true;
	}
	
	public void saveComts( Map<String,Object> video){
		if(!video.containsKey(Constants.COMTS)){
			return;
		}
		List<Map<String, Object>> comments = (List<Map<String, Object>>)video.get(Constants.COMTS);
		video.remove(Constants.COMTS);
		for(Map<String, Object> comment:comments){
			UUID id = (UUID)comment.get(Constants.OBJECT_ID);
			if(id==null){
				String origId = (String)comment.get("origId");
				id = UUIDUtils.getMd5UUID(origId);
			}
			if(id==null){
				throw new RuntimeException("comment id is empty!");
			}
			comment.put(Constants.OBJECT_ID, id);
			comtsdao.insertComment(comment);
		}
	}
	
	public void saveUser( Map<String,Object> video){
		if(!video.containsKey(Constants.USER)){
			return;
		}
		Map<String, Object> user = (Map<String, Object>)video.get(Constants.USER);
		video.remove(Constants.USER);
		Object uid = user.get(Constants.OBJECT_ID);
		if(uid==null){
			uid = user.get(Constants.USER_ID);
		}
		if(uid==null){
			throw new RuntimeException("user id is empty!");
		}
		user.put(Constants.OBJECT_ID, uid);
		video.put("uid", uid);
		// save user
		userdao.saveUser(user);
	}
	
	public void deleteVideo(UUID videoId) {
		if(videoId==null){
			return;
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,videoId);
		if(videoCollecion==null){
			videoCollecion = this.getDBCollection(VideoCollection);
		}
		videoCollecion.findAndRemove(query);
	}
	
	/**
	 * check whether the image is exists
	 * @param tweetsId
	 * @return
	 */
	public boolean isVideoExists(UUID videoId){
		if(videoId==null){
			throw new RuntimeException("videoId Id is empty");
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,videoId);
		if(videoCollecion==null){
			videoCollecion = this.getDBCollection(VideoCollection);
		}
		boolean hasNext = videoCollecion.find(query).hasNext();
		return hasNext;
	}
}
