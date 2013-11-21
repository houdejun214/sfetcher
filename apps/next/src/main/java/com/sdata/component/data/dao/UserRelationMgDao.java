package com.sdata.component.data.dao;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.mongo.MongoDao;
import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.GroupCommand;
import com.sdata.core.Constants;
import com.sdata.core.util.EmailUtil;

/**
 * 
 * user relationship data access object to mongodb
 * 
 * @author houdj
 *
 */
public class UserRelationMgDao extends MongoDao {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.UserRelationMgDao");
	
	private static final String userRelationCollection = "users.relation";
	
	private DBCollection dbCollection;
	
	private UserMgDao userdao = new UserMgDao();

	private static final BasicDBObject EmptyDoc = new BasicDBObject("$set",new BasicDBObject());
	
	@Override
	public void initilize(String host, int port, String dbName) {
		super.initilize(host, port, dbName);
		userdao.initilize(this.m, this.db);
	}

	public void saveUserRelation(Map<String,Object> userRelation,boolean saveRelationUser){
		if(dbCollection==null){
			dbCollection = this.getDBCollection(userRelationCollection);
			BasicDBObject keys = new BasicDBObject("uid",1);
			dbCollection.ensureIndex(keys, new BasicDBObject("unique" ,true));
		}
		Object uid = userRelation.get(Constants.UID);
		if(userRelation.containsKey(Constants.USER)){
			Map<String, Object> user = (Map<String, Object>) userRelation.get(Constants.USER);
			user.put(Constants.OBJECT_ID, uid);
			saveUser(user);
			userRelation.remove(Constants.USER);
		}
		List<Object> users = null;
		if(userRelation.containsKey(Constants.USER_LISTENS)){
			users = saveListenUsers(uid,(List<Map<String, Object>>) userRelation.get(Constants.USER_LISTENS),saveRelationUser);
			userRelation.remove(Constants.USER_LISTENS);
		}
		// save user listen relation
		saveUserListenRelation(uid,new BasicDBObject("$each",users));
	}
	
	private List<Object> saveListenUsers(Object curUserId, List<Map<String,Object>> users, boolean saveRelationUser){
		List<Object> userIdList = new ArrayList<Object>();
		for(Map<String,Object> user:users){
			if(!checkObjectId(user)){
				continue;
			}
			Object lisUserId = user.get(Constants.OBJECT_ID);
			if(curUserId==null){
				throw new RuntimeException("curUserId is null when save saveListenUsers");
			}
			/**
			 * save user follow relation ship for a single user
			 *
			 */
			if(saveRelationUser){
				saveUser(user); // save user, the method should be used in first crawl
			}
			userIdList.add(lisUserId);
			// create the user relation record if it doesn't exists
			BasicDBObject q = new BasicDBObject("uid",lisUserId);
			try{
				dbCollection.update(q,EmptyDoc,true,false);
			}catch(Exception e){
				log.warn("duplicated user and discard");
			}
		}
		
		try {
			findUsersAddFollows(userIdList,curUserId);
		} catch (Exception e) {
			log.warn(e.getMessage(),e);
			// exception arised, save relation for each single user
//			for(Map<String,Object> user:users){
//				Object lisUserId = user.get(Constants.OBJECT_ID);
//				saveUserFollowRelation(lisUserId,null,curUserId);
//			}
		}
		
		return userIdList;
	}
	
	
	/**
	 * check and make sure that every user have a objectId
	 * @param user
	 */
	private boolean checkObjectId(Map<String,Object> user){
		if(!user.containsKey(Constants.OBJECT_ID)){
			String name = (String)user.get(Constants.USER_NAME);
			if(StringUtils.isEmpty(name)){
				return false;
			}
			UUID id = StringUtils.getMd5UUID(name);
			user.put(Constants.OBJECT_ID, id);
		}
		return true;
	}
	
	private void findUsersAddFollows(List<Object> userIdList,Object followUserId){
		BasicDBObject query = new BasicDBObject();
		query.put("uid",new BasicDBObject("$in",userIdList));
		BasicDBObject doc = new BasicDBObject("$addToSet",new BasicDBObject(Constants.USER_FOLLOWS,followUserId));
		dbCollection.update(query, doc, true, true );
	}
	
	public void saveUserFollowRelation(Object curUserId,Object name,Object followUserId){
		BasicDBObject query = new BasicDBObject(Constants.UID,curUserId);
		query.append("cur", true);
		Map<String, ?> last = getDoc(query);
		BasicDBObject doc = new BasicDBObject();
		doc.put("uid", curUserId);
		//doc.put("name",name);
		int length = getInt(last,"sum",0);
		int seq = getInt(last,"seq",1);
		if(length>=100000){
			try {
				// update current row to a no-current row;
				dbCollection.findAndModify(query, null, null, false,  new BasicDBObject("$set",new BasicDBObject("cur",false)), false, true );
				// save new record
				seq++;
				doc.append("seq", seq).append("cur", true);
				dbCollection.findAndModify(query, null, null, false,  new BasicDBObject("$set",doc), false, true );
			} catch (Exception e) {
				EmailUtil.send("Tencent UserRelation Exception", "MongoDB has something wrong when insert name:["+name+"]'s followList that big than 100000.");
			}finally{
				EmailUtil.send("Tencent UserRelation success save more than 100000 follows", "MongoDB has success save name:["+name+"]'s followList that big than 100000.");
			}
		}else{
			dbCollection.findAndModify(query, null, null, false,  new BasicDBObject("$set",doc.append("cur",true).append("seq", seq)), false, true );
			doc = new BasicDBObject("$addToSet",new BasicDBObject(Constants.USER_FOLLOWS,followUserId));
			dbCollection.findAndModify(query, null, null, false, doc, false, true );
		}
	}
	
	private Map<String,?> getDoc(BasicDBObject query){
		BasicDBObject initial = new BasicDBObject();
		initial.put("sum", 0);
		StringBuffer reduce = new StringBuffer();
		reduce.append(" function(doc, out) {");
		reduce.append("  out.uid = doc.uid; ");
		reduce.append("  out.seq = doc.seq; ");
		reduce.append("  if(doc.follows!=null){");
		reduce.append("   out.sum=doc.follows.length;}");
		reduce.append(" }");
		GroupCommand cmd = new GroupCommand(dbCollection, null, query, initial,reduce.toString(),null);
		BasicDBList group = (BasicDBList) dbCollection.group(cmd);
		Map map = new HashMap();
		if(group !=null && group.size()!=0){
			map = (Map)group.get(0);
		}
		return map;
	}
	
	
	private int getInt(Map<String, ?> map,String key,int def){
		if(map.containsKey(key)){
			String value = String.valueOf(map.get(key));
			BigDecimal value_b = new BigDecimal(value);
			return value_b.intValue();
		}
		return def;
	}
	
	public void saveUserListenRelation(Object curUserId,Object listenUserIds){
		if(curUserId==null){
			throw new RuntimeException("uid is null when save userListenRelation");
		}
		BasicDBObject query = new BasicDBObject(Constants.UID,curUserId);
		BasicDBObject doc = new BasicDBObject("$addToSet",new BasicDBObject(Constants.USER_LISTENS,listenUserIds));
		dbCollection.findAndModify(query, null, null, false, doc, false, true );
	}
	
	/**
	 * insert a new user
	 * @param tweet
	 */
	public void saveUser( Map<String,Object> user){
		// change date string to date
		Object uid = user.get(Constants.OBJECT_ID);
		if(uid==null){
			uid = user.get(Constants.USER_ID);
		}
		if(uid==null){
			throw new RuntimeException("user id is empty!");
		}
		user.put(Constants.OBJECT_ID, uid);
		user.put(Constants.USER_ID, uid);
		user.put("uname", user.get("name"));
		user.put("sname", user.get("sname"));
		// remove tweet information, this is store in the tweets collection;
		user.remove("tweet");
		// save user
		userdao.saveUser(user);
	}
	
	public boolean isUserExists(Object userId){
		return userdao.isExists(userId);
	}
	
	public void dropRelationCollection(){
		if(dbCollection==null){
			dbCollection = this.getDBCollection(userRelationCollection);
		}
		dbCollection.drop();
		dbCollection = null;
	}

}
