package com.sdata.hot.image;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;

/**
 * @author zhufb
 *
 */
public class ImageFetcher extends SdataFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.ImageFetcher");
	public ImageFetcher(Configuration conf, RunState state) {
		this.parser = new ImageParser(conf);
	}
	
	@Override
	public List<FetchDatum> fetchDatumList() {
		ParseResult parseList = parser.parseList(null);
		log.warn("Got hot image size："+parseList.getFetchList().size());
		return parseList.getFetchList();
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum){
		log.warn("Got hot image："+datum.getUrl());
		return datum;
	}
	
	public boolean isComplete(){
		return true;
	}
}
