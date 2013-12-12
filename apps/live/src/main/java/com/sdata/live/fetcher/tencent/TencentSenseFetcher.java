package com.sdata.live.fetcher.tencent;

import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.time.DateFormat;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.item.CrawlItemEnum;
import com.sdata.core.parser.ParseResult;
import com.sdata.sense.SenseConfig;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.fetcher.SenseFetcher;
import com.sdata.sense.item.SenseCrawlItem;
import com.sdata.sense.store.SenseStorer;

/**
 * @author zhufb
 *
 */
public class TencentSenseFetcher extends SenseFetcher {
	public final static String FID = "tencent";
	private static final Logger log = LoggerFactory.getLogger("Sense.TencentSenseFetcher");

	public TencentSenseFetcher(Configuration conf, RunState state) {
		super(conf, state);
	}

	@Override
	public boolean isComplete() {
		return false;
	}

	@Override
	public void fetchDatumList(FetchDispatch fetchDispatch, SenseCrawlItem crawlItem) {
		Configuration conf = SenseConfig.getConfig(crawlItem);
		this.parser = TencentSenseParser.getTencentSenseFrom(conf,crawlItem);
		if(crawlItem.containParam(CrawlItemEnum.KEYWORD.getName())){
			this.fetchWithTime(fetchDispatch,crawlItem);
		}else{
			this.fetchNoTime(fetchDispatch,crawlItem);
		}
		
	}
	
	private void fetchWithTime(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem){
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
			TencentCrawlState state = new TencentCrawlState(1,curStart,endTime);
			end = this.fetch(fetchDispatch,crawlItem, state);
			endTime = curStart;
		}
	}

	private void fetchNoTime(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem){
		TencentCrawlState state = new TencentCrawlState();
		this.fetch(fetchDispatch, crawlItem, state);
	}
	
	private boolean fetch(FetchDispatch fetchDispatch,SenseCrawlItem crawlItem,TencentCrawlState state){
		boolean complete = false;
		while (!complete) {
			List<FetchDatum> data = ((TencentSenseParser)parser).getList(crawlItem, state);
			log.info("We has crawled tweets: "+ data.size() + ",param:" +crawlItem.getParamStr() + ",state :"+state);
			ParseResult result = new ParseResult();
			result.setFetchList(data);
			complete = ((TencentSenseParser)parser).isComplete();
			boolean end = this.end(result,crawlItem,state);
			fetchDispatch.dispatch(data);
			((TencentSenseParser)parser).next(state);
			if(end){
				return true;
			}
		}
		return false;
	}

	protected boolean end(ParseResult result,SenseCrawlItem item,TencentCrawlState state){
		List<FetchDatum> list = result.getFetchList();
		if(list !=null&&list.size()> 0){
			SenseStorer senseStore = parser.getSenseStore(item);
			Boolean incrementCrawl = this.getConfBoolean("sense.crawl.increment", false);
			if(incrementCrawl){
				for(FetchDatum d:list){
					//碰到一条转发微博，且在数据库中存在了，则可停止。
					Object opubTime = DateFormat.changeStrToDate(d.getMeta("timestamp"));
					String retid = d.getMeta(Constants.TWEET_RETWEETED_ID);
					if(opubTime!=null&&opubTime instanceof Date&&state.getEnd().after((Date)opubTime)&&state.getStart().before((Date)opubTime)&&!StringUtils.isEmpty(retid)){
						return senseStore.isExists((SenseFetchDatum)d);
					}
				}
			}
		}
		return false;
	}
	
	
	@Override
	public SenseFetchDatum fetchDatum(SenseFetchDatum datum) {
		Configuration conf = SenseConfig.getConfig(datum.getCrawlItem());
		
		TencentSenseParser downloader = TencentSenseParser.getTencentSenseFrom(conf,datum.getCrawlItem());
		datum = downloader.getDatum(datum);
		return datum;
	}
}
