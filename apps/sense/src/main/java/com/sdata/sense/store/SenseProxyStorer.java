package com.sdata.sense.store;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.SdataStorer;
import com.sdata.sense.SenseFactory;
import com.sdata.sense.SenseFetchDatum;

public class SenseProxyStorer extends SdataStorer {

	public SenseProxyStorer(Configuration conf,RunState state){
		this.setConf(conf);
	}
	
	@Override
	public void save(FetchDatum datum) throws Exception {
		SenseFetchDatum senseDatum = (SenseFetchDatum) datum;
		SenseStorer store = SenseFactory.getStorer(senseDatum.getCrawlItem().getCrawlerName());
		store.save(datum);
	}
}
