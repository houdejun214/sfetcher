package com.sdata.proxy.filter;


import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.filter.SdataFilter;
import com.sdata.proxy.SenseFactory;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.store.SenseStorer;

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
