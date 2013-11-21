package com.sdata.component.data;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.sdata.component.data.dao.GroupMgDao;
import com.sdata.component.data.dao.ImageMgDao;
import com.sdata.component.data.dao.LeafUserMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.RunState;
import com.sdata.core.data.FieldProcess;
import com.sdata.core.data.SdataStorer;
import com.sdata.core.parser.ParseResult;
import com.sdata.core.util.WebPageDownloader;

/**
 * save user relationship to database
 * 
 * @author houdj
 *
 */
public class SdataFlickrUserDetailDbStorer extends SdataStorer {
	
	private static final Logger log = LoggerFactory.getLogger("SdataFlickrUserRelationDbStorer");
	
	private UserMgDao userdao = new UserMgDao();
	private LeafUserMgDao leafuserdao = new LeafUserMgDao();
	private ImageMgDao imagedao = null;
	private GroupMgDao groupdao = new GroupMgDao();
	private FieldProcess fieldProcess ;
	public SdataFlickrUserDetailDbStorer(Configuration conf,RunState state){
		this.setConf(conf);
		this.state = state;
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		userdao.initilize(host,port,dbName);
		imagedao = new ImageMgDao(conf.get(Constants.SOURCE));
		groupdao.initilize(host, port, dbName);
		leafuserdao.initilize(host, port, dbName);
		fieldProcess = new FieldProcess(conf);
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		String name = datum.getName();
		Map<String, Object> map = datum.getMetadata();
		if(Constants.FLICKR_IMAGE.equals(name)){
			map = fieldProcess.fieldReduce(map);
			map.put(Constants.FETCH_TIME, new Date());
			imagedao.saveImage(map,fieldProcess);
		}else if(Constants.FLICKR_USER.equals(name)){
			map = fieldProcess.fieldReduce(map);
			Object id = map.get(Constants.OBJECT_ID);
			if(id ==null ){
				throw new RuntimeException("the property of userId is empty!");
			}else if(!(id instanceof Long || id instanceof UUID)){
				String tweetsId = StringUtils.valueOf(id);
				if("".equals(tweetsId))
					throw new RuntimeException("the property of userId is empty!");
				id = Long.valueOf(tweetsId);
				map.put(Constants.OBJECT_ID, id);
			}
			if(userdao.isExists(id)){
				userdao.saveUser(map);
			}else{
				leafuserdao.saveUser(map);
			}
		}else if(Constants.FLICKR_GROUP.equals(name)){
			map = fieldProcess.fieldReduce(map);
			groupdao.saveGroup(map);
		}
	}

}
