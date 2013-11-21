package com.sdata.sense.parser.statistic;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDispatch;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public interface ISenseStatistic {
	
	public void statistic(FetchDispatch fetchDispatch,Configuration conf,SenseCrawlItem item);
}
