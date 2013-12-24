package com.sdata.proxy.fetcher;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.download.http.HttpPageLoader;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RawContent;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.parser.ParseResult;
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.parser.SenseParser;
import com.sdata.proxy.store.SenseStorer;

/**
 * @author zhufb
 *
 */

public abstract class SenseFetcher extends SdataFetcher {
	
	public final static String FID = null;
	protected static Logger log = LoggerFactory.getLogger("Sense.SenseFetcher");
	protected SenseParser parser;
	protected static HttpPageLoader advancePageLoader = HttpPageLoader.getAdvancePageLoader();
	protected Map<Long,Boolean> map = new ConcurrentHashMap<Long,Boolean>();
	
	protected SenseFetcher(Configuration conf,RunState state) {
		super.setConf(conf);
		super.setRunState(state);
	}

	protected RawContent getRawContent(String url){
		if(StringUtils.isEmpty(url)){
			return null;
		}
		String content = advancePageLoader.download(url).getContentHtml();
		if(content == null){
			return null;
		}
		return new RawContent(url,content);
	}
	
	public boolean isComplete(SenseCrawlItem item){
		Boolean result = map.get(item.getId());
		return result==null?false:result;
	}
	
	public void complete(SenseCrawlItem item){
		map.put(item.getId(), true);
	}
	
	/**
	 * abstract for fetch datum list 
	 * 
	 * @param fetchDispatch
	 * @param crawlItem
	 */
	public abstract void fetchDatumList(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem);

	/**
	 * check fetch datum list stop or no
	 * 
	 * @param result
	 * @param item
	 * @return
	 */
	protected boolean end(ParseResult result,SenseCrawlItem item){
		if(isComplete(item)){
			return true;
		}
		List<FetchDatum> list = result.getFetchList();
		if(list == null||list.size() == 0){
			return true;
		}
		SenseStorer senseStore = parser.getSenseStore(item);
		
		// for increase crawl if repeat stop 
		boolean increase = this.getConfBoolean("crawler.increase", true);
		if(increase){
			return senseStore.isExists((SenseFetchDatum) list.get(list.size() - 1));
		}
		return false;
	}
	
	/**
	 * Get one datum 
	 * 
	 * @param datum
	 * @return
	 */
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum){
		if(datum == null){
			return null;
		}
		SenseCrawlItem item = datum.getCrawlItem();
		log.warn("fetch datum:"+ datum.getUrl());
		Configuration conf = SenseConfig.getConfig(item);
		datum = parser.parseDatum(datum,conf, this.getRawContent(datum.getUrl()));
		return datum;
	}
	
}
