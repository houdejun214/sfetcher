package com.sdata.hot.social;

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
public class SocialFetcher extends SdataFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.SocialFetcher");
	public SocialFetcher(Configuration conf, RunState state) {
		this.parser = new SocialParser(conf);
	}
	
	@Override
	public List<FetchDatum> fetchDatumList() {
		ParseResult parseList = parser.parseList(null);
		log.warn("Got hot social topic size："+parseList.getFetchList().size());
		return parseList.getFetchList();
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum){
		((SocialParser)parser).parse(datum);
		log.warn("Got hot social topic ："+datum.getUrl());
		return datum;
	}
	
	public boolean isComplete(){
		return true;
	}
}
