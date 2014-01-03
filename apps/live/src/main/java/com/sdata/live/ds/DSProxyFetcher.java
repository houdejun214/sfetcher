package com.sdata.live.ds;

import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.live.DBFactory;
import com.sdata.proxy.CrawlItemQueue;
import com.sdata.proxy.fetcher.SenseProxyFetcher;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class DSProxyFetcher extends SenseProxyFetcher {

	public DSProxyFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}

	@Override
	protected CrawlItemQueue initItemQueue() {
		return new CrawlItemQueue(DBFactory.getItemDB());
	}

	@Override
	protected SenseCrawlItem initItem(Map<String, Object> map) {
		return new DSItem(map);
	}
}
