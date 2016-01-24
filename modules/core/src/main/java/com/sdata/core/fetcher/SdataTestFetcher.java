package com.sdata.core.fetcher;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 此crawler不做任何数据抓取操作，仅作为测试使用。
 * @author houdejun
 *
 */
public class SdataTestFetcher extends SdataFetcher {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.SdataTestFetcher");
	
	public SdataTestFetcher(Configuration conf,RunState state){
		this.setConf(conf);
		this.setRunState(state);
	}


	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		this.await(2000);
		log.debug("fetch datum test");
		return null;
	}

	@Override
	public boolean isComplete() {
		return false;
	}

}
