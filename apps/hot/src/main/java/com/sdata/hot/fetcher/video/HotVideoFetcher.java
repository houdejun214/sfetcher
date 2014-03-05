package com.sdata.hot.fetcher.video;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.hot.Hot;
import com.sdata.hot.fetcher.HotBaseFetcher;

/**
 * @author zhufb
 *
 */
public abstract class HotVideoFetcher extends HotBaseFetcher{
	
	protected static Logger log = LoggerFactory.getLogger("Hot.HotVideoFetcher");

	protected HotVideoFetcher() {
		super();
	}
	
	public HotVideoFetcher(Configuration conf) {
		super(conf);
	}
	
	public Hot type() {
		return Hot.Video;
	}
}
