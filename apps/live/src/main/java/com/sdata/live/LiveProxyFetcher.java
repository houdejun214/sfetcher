package com.sdata.live;

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
public class LiveProxyFetcher extends SenseProxyFetcher {

	public LiveProxyFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}

	@Override
	protected CrawlItemQueue initItemQueue() {
		return new CrawlItemQueue(new LiveItemDB(CrawlAppContext.conf));
	}

	@Override
	protected SenseCrawlItem initItem(Map<String, Object> map) {
		return new LiveItem(map);
	}
}
