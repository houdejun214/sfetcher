package com.sdata.hot.interest;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.RawContent;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.hot.util.HotUtils;

/**
 * @author zhufb
 *
 */
public class InterestFetcher extends SdataFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.InterestFetcher");
	private String HOST = "http://www.google.com.sg/trends/hottrends/atom/feed?pn=p5"; 
	public InterestFetcher(Configuration conf, RunState state) {
		this.parser = new InterestParser(conf);
	}
	
	@Override
	public List<FetchDatum> fetchDatumList() {
		RawContent rawContent = HotUtils.getRawContent(HOST);
		ParseResult parseList = parser.parseList(rawContent);
		log.warn("Got hot interest size："+parseList.getFetchList().size());
		return parseList.getFetchList();
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum){
		log.warn("Got hot interest:"+datum.getUrl());
		return datum;
	}
	
	public boolean isComplete(){
		return true;
	}
}
