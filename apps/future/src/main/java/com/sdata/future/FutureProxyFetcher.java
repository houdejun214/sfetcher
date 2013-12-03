package com.sdata.future;

import java.util.Map;

import com.sdata.core.Configuration;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.RunState;
import com.sdata.core.item.CrawlItemQueue;
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
