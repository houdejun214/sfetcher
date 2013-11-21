package com.sdata.component.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.component.data.dao.FlickrUserRelationMgDao;
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
public class SdataFlickrUserRelationDbStorer extends SdataStorer {
	
	private static final Logger log = LoggerFactory.getLogger("SdataFlickrUserRelationDbStorer");
	private FlickrUserRelationMgDao flickrUserRelationDao = new FlickrUserRelationMgDao();
	private FieldProcess fieldProcess;
	public SdataFlickrUserRelationDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.flickrUserRelationDao.initilize(host, port, dbName);
		fieldProcess = new FieldProcess(conf);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		boolean isEdge = datum.getMetaBoolean("isEdge",false);
		Map<String, Object> userRelation = datum.getMetadata();
		userRelation = fieldProcess.fieldReduce(userRelation);
		if(isEdge){
			if(userRelation.containsKey(Constants.FLICKR_CONTACTLIST)){
				List<Map<String, Object>> newList = removeNonExistsUsers((List<Map<String, Object>>) userRelation.get(Constants.USER_LISTENS));
				userRelation.put(Constants.USER_LISTENS,newList);
			}
		}
		flickrUserRelationDao.saveUserRelation(userRelation);
	}

	private List<Map<String, Object>> removeNonExistsUsers(List<Map<String, Object>> users) {
		List<Map<String, Object>> newList= new ArrayList<Map<String, Object>>();
		for(Map<String,Object> user:users){
			if(!user.containsKey(Constants.OBJECT_ID)){
				String name = (String)user.get(Constants.USER);
				if(StringUtils.isEmpty(name)){
					log.warn("user name is empty:"+user.toString());
					continue;
				}
				UUID id = StringUtils.getMd5UUID(name);
				user.put(Constants.OBJECT_ID, id);
			}
			// this is a edge user
			if(!flickrUserRelationDao.isUserExists(user.get(Constants.OBJECT_ID))){
				continue;
			}
			newList.add(user);
		}
		return newList;
	}
}
