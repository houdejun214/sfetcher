package com.sdata.live.store;

import com.sdata.core.Configuration;
import com.sdata.core.RunState;
import com.sdata.core.data.dao.DBStore;
import com.sdata.core.data.dao.StoreCollection;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.store.SenseStorer;

/**
 * @author zhufb
 *
 */
public class SenseStatisticStorer extends SenseStorer {

	public final static String SID = "statistic";
	
	public SenseStatisticStorer(Configuration conf,RunState state) {
		super(conf,state);
	}
	
	public boolean isExists(SenseFetchDatum datum) {
		Configuration conf = getConf(datum);
		StoreCollection sc = getMainCollection(conf);
		if(sc == null){
			return false;
		}
		DBStore dbStore = super.getDBStore(conf,sc.getName());
		return dbStore.isExists(datum.getId());
	}
}
