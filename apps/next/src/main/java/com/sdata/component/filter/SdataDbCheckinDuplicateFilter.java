package com.sdata.component.filter;

import com.sdata.component.data.dao.FoursquareCheckinDao;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.filter.SdataFilter;

public class SdataDbCheckinDuplicateFilter extends SdataFilter {

	private FoursquareCheckinDao checkinDao = null;
	
	private RunState state;
	
	public SdataDbCheckinDuplicateFilter(Configuration conf,RunState state) {
		super(conf);
		this.state = state;
		checkinDao = new FoursquareCheckinDao();
		String host = this.getConf("mongoHost");
		int port = this.getConfInt("mongoPort",27017);
		String dbName = this.getConf("mongoDbName");
		checkinDao.initilize(host, port, dbName);
	}
	
	public boolean filter(FetchDatum datum) {
		String id = datum.getId().toString();
		boolean exists = checkinDao.isCheckinExists(id);
		if(exists){
			state.addOneRepeatDiscard();
			return false;
		}else{
			return true;
		}
	}
}
