package com.sdata.component.data.dao;

import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.DBCollection;
import com.sdata.core.Constants;

/**
 * 
 * flickr user relationship data access object to mongodb
 * 
 * @author qiumm
 *
 */
public class FlickrUserRelationMgDao extends MongoDao {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.FlickrUserRelationMgDao");
	
	private static final String userRelationCollection = "users";
	
	private DBCollection dbCollection;
	
	private UserMgDao userdao = new UserMgDao();
	
	
	@Override
	public void initilize(String host, int port, String dbName) {
		super.initilize(host, port, dbName);
		userdao.initilize(this.m, this.db);
	}


	public void saveUserRelation(Map<String,Object> userRelation){
		if(dbCollection==null){
			dbCollection = this.getDBCollection(userRelationCollection);
		}
		Object id = userRelation.get(Constants.OBJECT_ID);
		if(id ==null ){
			throw new RuntimeException("the property of objectId is empty!");
		}else if(!(id instanceof Number || id instanceof UUID)){
			String userId = StringUtils.valueOf(id);
			if("".equals(userId))
				throw new RuntimeException("the property of objectId is empty!");
			id = Long.valueOf(userId);
			userRelation.put(Constants.OBJECT_ID, id);
		}
		if(userRelation.containsKey(Constants.USER)){
			Map<String, Object> user = (Map<String, Object>) userRelation.get(Constants.USER);
			user.put(Constants.OBJECT_ID, id);
			
			String contactId = StringUtils.valueOf(user.get("nsid"));
			if(!StringUtils.isEmpty(contactId)){
				String idStr = contactId.replace("@", "0").replace("N", "1");
				Long idL = Long.valueOf(idStr);
				if(!userdao.isExists(idL)){
					log.info("success save add id:["+contactId+"]'s user info.");
				}
			}
			saveUser(user);
			userRelation.remove(Constants.USER);
		}
	}
	
	/**
	 * insert a new user
	 * @param tweet
	 */
	public void saveUser( Map<String,Object> user){
		// save user
		userdao.saveUser(user);
	}
	
	public boolean isUserExists(Object userId){
		return userdao.isExists(userId);
	}
}
