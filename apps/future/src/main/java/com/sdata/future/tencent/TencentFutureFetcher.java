package com.sdata.future.tencent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.parser.ParseResult;
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.fetcher.SenseFetcher;
import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class TencentFutureFetcher extends SenseFetcher {
	public final static String FID = "tencentFuture";
	protected static final Logger log = LoggerFactory.getLogger("Future.TencentFutureFetcher");
	public TencentFutureFetcher(Configuration conf,RunState state) {
		super(conf,state);
		super.parser = new TencentFutureParser(conf);
	}

	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		ParseResult result = parser.parseCrawlItem(conf, null, crawlItem);
		fetchDispatch.dispatch(result.getFetchList());
	}
	
	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum){
		if(datum == null){
			return null;
		}
		SenseCrawlItem item = datum.getCrawlItem();
		Configuration conf = SenseConfig.getConfig(item);
		parser.parseDatum(datum,conf, null);
		return datum;
	}
}
