package com.sdata.sense.store;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.data.store.SdataStorer;
import com.sdata.sense.SenseFactory;
import com.sdata.sense.SenseFetchDatum;

/**
 * @author zhufb
 *
 */
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
