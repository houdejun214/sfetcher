package com.sdata.live.fetcher;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.live.state.LiveState;
import com.sdata.proxy.SenseConfig;
import com.sdata.proxy.SenseFactory;
import com.sdata.proxy.SenseFetchDatum;
import com.sdata.proxy.fetcher.SenseFetcher;
import com.sdata.proxy.item.SenseCrawlItem;
import com.sdata.proxy.store.SenseStorer;

/**
 * Live Fetcher with time roll and resource
 * 
 * 
 * @author zhufb
 *
 */
public abstract class LiveFetcherWithTime extends SenseFetcher {
	
	private static final Logger log = LoggerFactory.getLogger("Live.LiveFetcherWithTime");
	
	private LiveBaseWithTime fetcher;

	protected abstract LiveBaseWithTime initLiveFetcher(SenseCrawlItem item,Configuration conf );
	
	protected LiveFetcherWithTime(Configuration conf, RunState state) {
		super(conf, state);
	}
	
	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		this.fetcher = initLiveFetcher(crawlItem,conf);
		if(crawlItem.containParam(CrawlItemEnum.KEYWORD.getName())){
			this.fetchWithTime(fetchDispatch,crawlItem);
		}else{
			this.fetchNoTime(fetchDispatch,crawlItem);
		}
	}

	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum) {
		SenseCrawlItem crawlItem = datum.getCrawlItem();
		Configuration conf = SenseConfig.getConfig(crawlItem);
		// must local param for thread safe
		LiveBaseWithTime datumFetcher = initLiveFetcher(crawlItem,conf);
		return datumFetcher.getDatum(datum);
	}
	
	
	protected void fetchNoTime(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem){
		LiveState state = new LiveState();
		this.fetch(fetchDispatch, crawlItem, state);
	}

	protected void fetchWithTime(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem){
		Date endTime = crawlItem.getEnd();
		Date start = crawlItem.getStart();
		if(endTime ==null){
			endTime = new Date();
		}
		boolean end  = false;
		while(!end&&DateTimeUtils.compareDate(start,endTime) == -1){
			Date curStart = DateTimeUtils.add(endTime, Calendar.HOUR, -1);
			if(curStart.compareTo(start) != 1){
				curStart = start;
			}
			LiveState state = new LiveState(1,curStart,endTime);
			end = this.fetch(fetchDispatch,crawlItem, state);
			endTime = curStart;
		}
	}

	protected boolean fetch(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem,LiveState state){
		boolean complete = false;
		while (!complete) {
			List<FetchDatum> data = fetcher.getList(crawlItem, state);
			log.info("We have crawled tweets: "+ data.size() + ",param:" +crawlItem.getParamStr() + ",state :"+state);
			complete = fetcher.isComplete();
			boolean end = this.end(data,crawlItem,state);
			fetchDispatch.dispatch(data);
			if(end){
				return true;
			}
			fetcher.next(state);
		}
		return false;
	}
	
	protected boolean end(List<FetchDatum> list,SenseCrawlItem item,LiveState state){
		if(list !=null&&list.size()> 0){
			SenseStorer senseStore = SenseFactory.getStorer(item.getCrawlerName());
			boolean increase = this.getConfBoolean("crawler.increase", true);
			if(increase){
				return senseStore.isExists((SenseFetchDatum)list.get(0));
			}
		}
		return false;
	}
	
	@Override
	public boolean isComplete(){
		return false;
	}
}
