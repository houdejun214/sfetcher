package com.sdata.future;

import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.state.RunState;
import com.sdata.core.CrawlItemQueue;
import com.sdata.sense.fetcher.SenseProxyFetcher;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class FutureProxyFetcher extends SenseProxyFetcher {

	public FutureProxyFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}

	@Override
	protected CrawlItemQueue initItemQueue() {
		return new CrawlItemQueue(new FutureItemDB(CrawlAppContext.conf));
	}

	@Override
	protected SenseCrawlItem initItem(Map<String, Object> map) {
		return new FutureItem(map);
	}
}
