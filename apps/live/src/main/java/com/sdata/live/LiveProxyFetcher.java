package com.sdata.live;

import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.sense.SenseItemQueue;
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
	protected SenseItemQueue initItemQueue() {
		return new SenseItemQueue(DBFactory.getItemDB());
	}

	@Override
	protected SenseCrawlItem initItem(Map<String, Object> map) {
		return new LiveItem(map);
	}
}
