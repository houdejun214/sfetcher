package com.sdata.live.history;

import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.state.RunState;
import com.sdata.core.CrawlItemQueue;
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
