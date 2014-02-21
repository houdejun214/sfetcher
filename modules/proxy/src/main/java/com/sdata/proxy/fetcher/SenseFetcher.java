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
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.SenseFactory;
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
			return new RawContent(url,null);
		}
		String content = null;
		int i = 0; 
		while(StringUtils.isEmpty(content)&&i<3){
			this.await(i*1000);
			content = advancePageLoader.download(url).getContentHtml();
			i++;
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
	 * @param list
	 * @param item
	 * @return
	 */
	protected boolean end(List<FetchDatum> list,SenseCrawlItem item){
		if(list == null||list.size() == 0){
			return true;
		}
		return this.end((SenseFetchDatum) list.get(list.size() - 1), item);
	}
	
	/**
	 * check fetch datum list stop or no
	 * 
	 * @param list
	 * @param item
	 * @return
	 */
	protected boolean end(SenseFetchDatum datum,SenseCrawlItem item){
		if(isComplete(item)){
			return true;
		}
		SenseStorer senseStore = SenseFactory.getStorer(StringUtils.valueOf(item.getId()));
		// for increase crawl if repeat stop 
		boolean increase = this.getConfBoolean("crawler.increase", true);
		if(increase){
			return senseStore.isExists(datum);
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
