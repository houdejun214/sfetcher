package com.sdata.common;

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
public class CommonProxyFetcher extends SenseProxyFetcher {

	public CommonProxyFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}

	@Override
	protected CrawlItemQueue initItemQueue() {
		return new CrawlItemQueue(CommonItemDB.getInstance());
	}

	@Override
	protected SenseCrawlItem initItem(Map<String, Object> map) {
		return new CommonItem(map);
	}
}
