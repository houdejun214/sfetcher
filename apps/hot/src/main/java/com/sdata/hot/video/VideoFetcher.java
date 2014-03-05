package com.sdata.hot.video;

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
public class VideoFetcher extends SdataFetcher {

	protected static Logger log = LoggerFactory.getLogger("Hot.VideoFetcher");
	private String HOST = "http://gdata.youtube.com/feeds/api/standardfeeds/SG/most_popular?time=today"; 
	public VideoFetcher(Configuration conf, RunState state) {
		this.parser = new VideoParser(conf);
	}
	
	@Override
	public List<FetchDatum> fetchDatumList() {
		RawContent rawContent = HotUtils.getRawContent(HOST);
		ParseResult parseList = parser.parseList(rawContent);
		log.warn("Got hot video size："+parseList.getFetchList().size());
		return parseList.getFetchList();
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum){
		log.warn("Got hot video:"+datum.getUrl());
		return datum;
	}
	
	public boolean isComplete(){
		return true;
	}
}
