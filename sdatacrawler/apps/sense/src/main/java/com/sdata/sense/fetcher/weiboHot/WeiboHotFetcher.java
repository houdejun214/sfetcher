package com.sdata.sense.fetcher.weiboHot;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.core.Configuration;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RunState;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.SenseConfig;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.fetcher.SenseFetcher;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.parser.weiboHot.WeiboHotParser;

/**
 * @author zhufb
 *
 */
public class WeiboHotFetcher extends SenseFetcher {
	public final static String FID = "weiboHot";
	protected static final Logger log = LoggerFactory.getLogger("Sense.WeiboHotFetcher");
	public WeiboHotFetcher(Configuration conf,RunState state) {
		super(conf,state);
		super.parser = new WeiboHotParser(conf);
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
