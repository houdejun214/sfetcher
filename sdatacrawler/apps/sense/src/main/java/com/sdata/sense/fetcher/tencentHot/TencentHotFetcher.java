package com.sdata.sense.fetcher.tencentHot;

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
import com.sdata.sense.parser.tencentHot.TencentHotParser;

/**
 * @author zhufb
 *
 */
public class TencentHotFetcher extends SenseFetcher {
	public final static String FID = "tencentHot";
	protected static final Logger log = LoggerFactory.getLogger("Sense.TencentHotFetcher");
	public TencentHotFetcher(Configuration conf,RunState state) {
		super(conf,state);
		super.parser = new TencentHotParser(conf);
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
