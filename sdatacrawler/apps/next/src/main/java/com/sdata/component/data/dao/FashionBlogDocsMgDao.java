package com.sdata.component.data.dao;

import java.util.Map;
import java.util.UUID;

import org.jsoup.nodes.Document;

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
public class FashionBlogDocsMgDao extends MongoDao {
	
	private static final String docsCollectionName="docs";
	
	private DBCollection docsCollection;
	
	@Override
	public void initilize(String host, int port, String dbName) {
		super.initilize(host, port, dbName);
	}
	
	public boolean saveBlogDocDetail(Map<String, Object> fashionBlog){
		String pageUrl =StringUtils.valueOf(fashionBlog.get("pageUrl"));
		if(pageUrl == null ){
			throw new RuntimeException("the property of Id is empty!");
		}
		UUID id = UUIDUtils.getMd5UUID(pageUrl);
		if(docsCollection==null){
			docsCollection = getDBCollection(docsCollectionName);
		}
		//save video
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,id);
		BasicDBObject doc = new BasicDBObject();
		Document docDetail = (Document)fashionBlog.get("doc");
		if(docDetail!=null){
			doc.put("docDetail",docDetail.toString());
			doc.put("pageUrl", pageUrl);
			docsCollection.findAndModify( query, null, null, false, new BasicDBObject("$set",doc), false, true );
		}
		return true;
	}
}
