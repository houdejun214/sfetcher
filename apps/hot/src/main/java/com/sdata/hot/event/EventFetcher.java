package com.sdata.hot.event;

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
@Deprecated
public class EventFetcher extends SdataFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.EventFetcher");
	private String HOST = "http://singaporeseen.stomp.com.sg/singaporeseen/category/hot-topics"; 
	public EventFetcher(Configuration conf, RunState state) {
		this.parser = new EventParser(conf);
	}
	
	@Override
	public List<FetchDatum> fetchDatumList() {
		RawContent rawContent = HotUtils.getRawContent(HOST);
		ParseResult parseList = parser.parseList(rawContent);
		log.warn("Got hot event size："+parseList.getFetchList().size());
		return parseList.getFetchList();
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum){
		((EventParser)parser).parse(datum);
		log.warn("Got hot event："+datum.getUrl());
		return datum;
	}
	
	public boolean isComplete(){
		return true;
	}
}
