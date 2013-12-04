package com.sdata.live.store;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.db.BaseDao;
import com.sdata.db.DaoCollection;
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
		DaoCollection sc = getMainCollection(conf);
		if(sc == null){
			return false;
		}
		BaseDao dbStore = super.getDBStore(conf,sc.getName());
		return dbStore.isExists(datum.getId());
	}
}
