package com.sdata.hot.venue;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;

/**
 * @author zhufb
 *
 */
public class VenueFetcher extends SdataFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.VenueFetcher");
	public VenueFetcher(Configuration conf, RunState state) {
		this.parser = new VenueParser(conf);
	}
	@Override
	public List<FetchDatum> fetchDatumList() {
		ParseResult parseList = parser.parseList(null);
		log.warn("Got hot venue size："+parseList.getFetchList().size());
		return parseList.getFetchList();
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum){
		log.warn("Got hot venue："+datum.getUrl());
		return datum;
	}
	
	public boolean isComplete(){
		return true;
	}
}
