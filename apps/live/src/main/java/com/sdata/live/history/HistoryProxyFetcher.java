package com.sdata.live.history;

import java.util.Map;

import com.sdata.core.Configuration;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.RunState;
import com.sdata.core.item.CrawlItemQueue;
import com.sdata.live.LiveItem;
import com.sdata.sense.fetcher.SenseProxyFetcher;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class HistoryProxyFetcher extends SenseProxyFetcher {

	public HistoryProxyFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}

	@Override
	protected CrawlItemQueue initItemQueue() {
		return new CrawlItemQueue(new HistoryItemDB(CrawlAppContext.conf));
	}

	@Override
	protected SenseCrawlItem initItem(Map<String, Object> map) {
		return new LiveItem(map);
	}
}
