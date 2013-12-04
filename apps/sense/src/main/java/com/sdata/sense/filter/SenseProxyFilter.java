package com.sdata.sense.filter;


import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.filter.SdataFilter;
import com.sdata.sense.SenseFactory;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.store.SenseStorer;

/**
 * @author zhufb
 *
 */
public class SenseProxyFilter extends SdataFilter {
	
	public SenseProxyFilter(Configuration conf,RunState state) {
		super(conf);
	}
	
	public boolean filter(FetchDatum datum) {
		SenseFetchDatum senseDatum = (SenseFetchDatum) datum;
		SenseStorer store = SenseFactory.getStorer(senseDatum.getCrawlItem().getCrawlerName());
		return !store.isExists(senseDatum);
	}
}
