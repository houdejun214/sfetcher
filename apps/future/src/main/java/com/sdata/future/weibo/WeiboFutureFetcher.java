package com.sdata.future.weibo;


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
public class WeiboFutureFetcher extends SenseFetcher {
	public final static String FID = "weiboFuture";
	protected static final Logger log = LoggerFactory.getLogger("Future.WeiboFutureFetcher");
	public WeiboFutureFetcher(Configuration conf,RunState state) {
		super(conf,state);
		super.parser = new WeiboFutureParser(conf);
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
		return datum;
	}
}
