package com.sdata.component.word;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.item.CrawlItem;
import com.sdata.core.item.CrawlItemQueue;

public class WordFetcher extends SdataFetcher {

	private static final Logger log = LoggerFactory
			.getLogger("SdataCrawler.WordFetcher");
	protected CrawlItemQueue crawlItemQueue = null;
	public WordFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
		this.parser = WordParser.getWordParser(super.getConf());
		crawlItemQueue = CrawlItemQueue.getInstance();
	}

	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch) {
		Map<String,Object> map = crawlItemQueue.get();
		CrawlItem item = new CrawlItem(map);
		Date endTime = item.getEnd();
		Date start = item.getStart();
	    WordParser wp = WordParser.getWordParser(super.getConf());
		while(DateTimeUtils.compareDate(start,endTime) == -1){
			Date curEnd = DateTimeUtils.add(start, Calendar.HOUR, 1);
			if(curEnd.compareTo(endTime) != -1){
				curEnd = endTime;
			}
			boolean complete = false;
			String state = "";
			while (!complete) {
				List<FetchDatum> data = wp.getFetchList(item,start,curEnd,state);
				log.info("We has crawled tweets: " + data.size() + " word:" + item.getKeyword()+ " start time :"+ start+" ,end time:"+curEnd +",state:"+state);
				fetchDispatch.dispatch(data);
				state = wp.getCurrentState();
				complete = wp.isComplete();
				
			}
			start = curEnd;
		}
	}

	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return ((WordParser)parser).getFetchDatum(datum);
	}
}
