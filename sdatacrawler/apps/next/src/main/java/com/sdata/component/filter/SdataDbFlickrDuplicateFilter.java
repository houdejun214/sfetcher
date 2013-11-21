package com.sdata.component.filter;

import com.sdata.component.data.dao.GroupMgDao;
import com.sdata.component.data.dao.ImageMgDao;
import com.sdata.component.data.dao.LeafUserMgDao;
import com.sdata.component.data.dao.UserMgDao;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.filter.SdataFilter;

public class SdataDbFlickrDuplicateFilter extends SdataFilter {

	private UserMgDao userdao = new UserMgDao();
	private LeafUserMgDao leafuserdao = new LeafUserMgDao();
	private ImageMgDao imagedao = null;
	private GroupMgDao groupdao = new GroupMgDao();

	private RunState state;
	
	public SdataDbFlickrDuplicateFilter(Configuration conf,RunState state) {
		super(conf);
		this.state = state;
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		this.userdao.initilize(host, port, dbName);
		this.leafuserdao.initilize(host, port, dbName);
		this.imagedao = new ImageMgDao(conf.get(Constants.SOURCE));
		this.groupdao.initilize(host, port, dbName);
	}
	
	public boolean filter(FetchDatum datum) {
		String id = datum.getId().toString();
		String name = datum.getName();
		if(Constants.FLICKR_IMAGE.equals(name)){
			if(imagedao.isImageExists(id)){// if this image had exists in mongoDB,remove this image
				state.addOneRepeatDiscard();
				return false;
			}else{
				return true;
			}
		}else if(Constants.FLICKR_USER.equals(name)){
			String id_s = id.replace("@", "0").replace("N", "1");
			Long _id = Long.valueOf(id_s);
			if(userdao.isExists(_id)||leafuserdao.isExists(_id)){
				state.addOneRepeatDiscard();
				return false;
			}else{
				return true;
			}
		}else if(Constants.FLICKR_GROUP.equals(name)){
			String id_s = id.replace("@", "0").replace("N", "1");
			Long _id = Long.valueOf(id_s);
			if(groupdao.isExists(_id)){
				state.addOneRepeatDiscard();
				return false;
			}else{
				return true;
			}
		}else{
			return true;
		}
	}
}
