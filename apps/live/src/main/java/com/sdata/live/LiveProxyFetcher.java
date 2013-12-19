package com.sdata.live;

import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.proxy.CrawlItemQueue;
import com.sdata.proxy.fetcher.SenseProxyFetcher;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class LiveProxyFetcher extends SenseProxyFetcher {

	public LiveProxyFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}

	@Override
	protected CrawlItemQueue initItemQueue() {
		return new CrawlItemQueue(DBFactory.getItemDB());
	}

	@Override
	protected SenseCrawlItem initItem(Map<String, Object> map) {
		return new LiveItem(map);
	}
}
