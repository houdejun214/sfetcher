package com.sdata.component.data.dao;

import java.util.Map;
import java.util.UUID;

import org.springframework.stereotype.Repository;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;
import com.sdata.core.data.FieldProcess;

/**
 * save news data to mongodb database
 * 
 * @author qmm
 *
 */
@Repository
public class NewsMgDao extends MongoDao{
	private static final String ID=Constants.OBJECT_ID; // this id is for mongodb id field key.
	private static final String NewsCollection="news";
	private DBCollection twCollection;
//	private String core;
	
	FieldProcess process;
	
	@Override
	public void initilize(String host, int port, String dbName) {
//		core = dbName.concat(".").concat(NewsCollection);
		super.initilize(host, port, dbName);
	}

	/**
	 * insert a new news 
	 * @param news
	 */
	public void saveNews(Map<String,Object> news,FieldProcess fieldProcess){
		this.saveNews(news,fieldProcess,false);
	}
	
	/**
	 * save news data without index the data
	 * @param news
	 * @param fieldProcess
	 */
	public void saveNews(Map<String,Object> news,FieldProcess fieldProcess,boolean index){
		if(twCollection==null){
			twCollection = this.getDBCollection(NewsCollection);
		}
		if(process == null) this.process = fieldProcess;
		Object id = news.get(ID);
		if(id == null ){
			throw new RuntimeException("the property of tweetId is empty!");
		}
		BasicDBObject query = new BasicDBObject(ID,id);
		BasicDBObject doc = new BasicDBObject();
		doc.putAll(news);
		twCollection.findAndModify(query, null, null, false, doc, false, true);
		//create news index
		if(index){
			fieldProcess.solrIndex(news);
		}
	}
	
	/**
	 * delete a news by id
	 * @param newsId
	 */
	public void deleteNews(String newsId) {
		if(StringUtils.isEmpty(newsId)){
			throw new RuntimeException("the property of newsId is empty!");
		}
		BasicDBObject query = new BasicDBObject(Constants.OBJECT_ID,newsId);
		if(twCollection==null){
			twCollection = this.getDBCollection(NewsCollection);
		}
		twCollection.findAndRemove(query);
	}
	
	/**
	 * check whether the news is exists
	 * @param newsId
	 * @return
	 */
	public boolean isNewsExists(UUID newsId){
		if(newsId==null){
			throw new RuntimeException("newsId is empty");
		}
		BasicDBObject query = new BasicDBObject(ID,newsId);
		if(twCollection==null){
			twCollection = this.getDBCollection(NewsCollection);
		}
		boolean hasNext = twCollection.find(query).hasNext();
		return hasNext;
	}
}
