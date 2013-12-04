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
import com.sdata.sense.SenseConfig;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.fetcher.SenseFetcher;
import com.sdata.sense.item.SenseCrawlItem;

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
		String token = conf.get("weibo_token");
  		token = StringUtils.isEmpty(token)?state.getCrawlName():token;
		WeiboServer.init(token);
		Weibo.initToken();
		Map<String,String> header = new HashMap<String,String>();
		header.put("Cookie", Weibo.getCookie());
		WeiboSenseFrom senseFrom = WeiboSenseFromFactory.getSenseFrom(crawlItem,conf,header);
		List<FetchDatum> data = senseFrom.getData(crawlItem);
		log.warn("fetch weibo :"+crawlItem.getEntryUrl()+" tweets size:"+data.size());
		fetchDispatch.dispatch(data);
	}

	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum) {
		Map<String,String> header = new HashMap<String,String>();
		header.put("Cookie", Weibo.getCookie());
		Configuration conf = SenseConfig.getConfig(datum.getCrawlItem());
		WeiboSenseFrom senseFrom = WeiboSenseFromFactory.getSenseFrom(datum.getCrawlItem(),conf,header);
		datum = senseFrom.getDatum(datum);
		return datum;
	}

	@Override
	public boolean isComplete(){
		return false;
	}
}
