package com.sdata.component.data.dao;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;

/**
 * @author houdj
 *
 */
public class FashionBlogsMgDao extends MongoDao {
	
	private static final String bolgsCollectionName="blogs";
	
	private DBCollection bolgsCollection;
	
	private FashionBlogDocsMgDao docDao = new FashionBlogDocsMgDao();
	
	@Override
	public void initilize(String host, int port, String dbName) {
		super.initilize(host, port, dbName);
		docDao.initilize(host, port, dbName);
	}
	
	public boolean saveBlogs(Map<String, Object> fashionBlog){
		String pageUrl =StringUtils.valueOf(fashionBlog.get("pageUrl"));
		if(pageUrl == null ){
			throw new RuntimeException("the property of Id is empty!");
		}
		//save doc details
		docDao.saveBlogDocDetail(fashionBlog);
		fashionBlog.remove("doc");
		//check images
		List<String> imagesList = (List<String>)fashionBlog.get("imgs");
		if(imagesList==null || imagesList.size()==0){
			return true;
		}
		UUID id = UUIDUtils.getMd5UUID(pageUrl);
		if(bolgsCollection==null){
			bolgsCollection = getDBCollection(bolgsCollectionName);
		}
		//save video
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		BasicDBObject doc = new BasicDBObject();
		fashionBlog.remove(Constants.OBJECT_ID);
		doc.putAll(fashionBlog);
		bolgsCollection.findAndModify( query, null, null, false, new BasicDBObject("$set",doc), false, true );
		return true;
	}
}
