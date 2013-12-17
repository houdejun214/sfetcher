package com.sdata.live.fetcher.weibo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import weibo4j.Weibo;
import weibo4j.util.WeiboServer;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.fetcher.SenseFetcher;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.resource.Resources;

/**
 * weibo sense fetcher implement
 * 
 * @author zhufb
 *
 */

public class WeiboSenseFetcher extends SenseFetcher{
	
	public final static String FID = "weibo";
	
	private static final Logger log = LoggerFactory.getLogger("Sense.WeiboSenseFetcher");
	public WeiboSenseFetcher(Configuration conf,RunState state) {
		super(conf,state);
	}

	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		WeiboSenseFrom senseFrom = WeiboSenseFromFactory.getSenseFrom(crawlItem,conf);
		List<FetchDatum> data = senseFrom.getData(crawlItem);
		log.warn("fetch weibo :"+crawlItem.getEntryUrl()+" tweets size:"+data.size());
		fetchDispatch.dispatch(data);
	}

	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum) {
		Configuration conf = SenseConfig.getConfig(datum.getCrawlItem());
		WeiboSenseFrom senseFrom = WeiboSenseFromFactory.getSenseFrom(datum.getCrawlItem(),conf);
		datum = senseFrom.getDatum(datum);
		return datum;
	}

	@Override
	public boolean isComplete(){
		return false;
	}
}
