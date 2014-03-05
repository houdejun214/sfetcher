package com.sdata.live.fetcher.tencent;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.live.fetcher.LiveBaseWithTime;
import com.sdata.live.fetcher.LiveFetcherWithTime;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class LiveTencentFetcher extends LiveFetcherWithTime {
	
	public final static String FID = "tencent";
	
	public LiveTencentFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}
	
	@Override
	protected LiveBaseWithTime initLiveFetcher(SenseCrawlItem item,
			Configuration conf) {
		return LiveTencentBase.getFetcher(conf, item);
	}

}
