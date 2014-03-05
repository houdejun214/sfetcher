package com.sdata.core.fetcher;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;

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
	public List<FetchDatum> fetchDatumList() {
		this.await(2000);
		ArrayList<FetchDatum> list = new ArrayList<FetchDatum>();
		list.add(new FetchDatum());
		log.debug("fetch datum list test");
		return list;
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
