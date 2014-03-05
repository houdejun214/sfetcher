package com.sdata.hot.fetcher.image;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.hot.Hot;
import com.sdata.hot.fetcher.HotBaseFetcher;

/**
 * @author zhufb
 *
 */
public abstract class HotImageFetcher extends HotBaseFetcher{

	protected static Logger log = LoggerFactory.getLogger("Hot.HotImageFetcher");

	protected HotImageFetcher() {
		super();
	}
	
	public HotImageFetcher(Configuration conf) {
		super(conf);
	}
	
	public Hot type(){
		return Hot.Image;
	}
}
