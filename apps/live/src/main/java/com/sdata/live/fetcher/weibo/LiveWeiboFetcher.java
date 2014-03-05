package com.sdata.live.fetcher.weibo;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.live.fetcher.LiveBaseWithTime;
import com.sdata.live.fetcher.LiveFetcherWithTime;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * weibo sense fetcher implement
 * 
 * @author zhufb
 *
 */

public class LiveWeiboFetcher extends LiveFetcherWithTime{
	
	public final static String FID = "weibo";
	public LiveWeiboFetcher(Configuration conf,RunState state) {
		super(conf,state);
	}

	@Override
	protected LiveBaseWithTime initLiveFetcher(SenseCrawlItem item,
			Configuration conf) {
		return LiveWeiboBase.getSenseFrom(item,conf);
	}
}
