package com.sdata.future;

import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.state.RunState;
import com.sdata.sense.SenseItemQueue;
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
	protected SenseItemQueue initItemQueue() {
		return new SenseItemQueue(new FutureItemDB(CrawlAppContext.conf));
	}

	@Override
	protected SenseCrawlItem initItem(Map<String, Object> map) {
		return new FutureItem(map);
	}
}
