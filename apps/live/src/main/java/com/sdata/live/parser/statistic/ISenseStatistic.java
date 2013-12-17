package com.sdata.live.parser.statistic;

import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDispatch;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public interface ISenseStatistic {
	
	public void statistic(FetchDispatch fetchDispatch,Configuration conf,SenseCrawlItem item);
}
