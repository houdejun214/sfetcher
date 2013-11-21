package com.sdata.component.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.StopWatch;
import com.lakeside.data.mongo.MongoDbSnapshot;
import com.sdata.component.data.dao.UserRelationMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;

/**
 * save user relationship to database
 * 
 * @author houdj
 *
 */
public class SdataUserRelationDbStorer extends SdataStorer {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataUserRelationDbStorer");
	
	private UserRelationMgDao userRelationDao = new UserRelationMgDao();
	
	private FieldProcess fieldProcess ;

	private String host;

	private String dbName;
	
	private String backupDir;
	
	public SdataUserRelationDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		host = this.getConf("mongoHost");
		backupDir = this.getConf("backupDir");
		int port = this.getConfInt("mongoPort",27017);
		dbName = this.getConf("mongoDbName");
		this.userRelationDao.initilize(host, port, dbName);
		fieldProcess = new FieldProcess(conf);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		StopWatch watch = new StopWatch();
		watch.start();
		boolean isEdge = datum.getMetaBoolean("isEdge",false);
		Map<String, Object> userRelation = datum.getMetadata();
		userRelation = fieldProcess.fieldReduce(userRelation);
		Object uid = userRelation.get("uid");
		if(uid!=null)
			userRelation.put(Constants.OBJECT_ID, uid);
		if(isEdge){
			if(userRelation.containsKey(Constants.USER_LISTENS)){
				List<Map<String, Object>> newList = removeNonExistsUsers((List<Map<String, Object>>) userRelation.get(Constants.USER_LISTENS));
				userRelation.put(Constants.USER_LISTENS,newList);
			}
		}
		// it indicate the user set is close when isEdge is true, so no need to save new users.
		boolean saveRelationUser = !isEdge;
		userRelationDao.saveUserRelation(userRelation,saveRelationUser);
		watch.stop();
		long elapsedTime = watch.getElapsedTime();
		if(elapsedTime>1000) {
			log.info("end saveUserRelation elapse {}",elapsedTime);
		}
	}

	private List<Map<String, Object>> removeNonExistsUsers(List<Map<String, Object>> users) {
		List<Map<String, Object>> newList= new ArrayList<Map<String, Object>>();
		for(Map<String,Object> user:users){
			if(!user.containsKey(Constants.OBJECT_ID)){
				String name = (String)user.get(Constants.USER_NAME);
				if(StringUtils.isEmpty(name)){
					log.warn("user name is empty:"+user.toString());
					continue;
				}
				UUID id = StringUtils.getMd5UUID(name);
				user.put(Constants.OBJECT_ID, id);
			}
			// this is a edge user
			if(!userRelationDao.isUserExists(user.get(Constants.OBJECT_ID))){
				continue;
			}
			newList.add(user);
		}
		return newList;
	}
	
	public void backup(){
		MongoDbSnapshot dbSnapshot = new MongoDbSnapshot(backupDir,host,dbName,"users.relation");
		dbSnapshot.start();
		
		this.userRelationDao.dropRelationCollection();
	}
}
