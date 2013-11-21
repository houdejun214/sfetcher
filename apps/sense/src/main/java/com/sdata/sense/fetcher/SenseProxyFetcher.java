package com.sdata.sense.fetcher;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Map;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.RunState;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.item.CrawlItemQueue;
import com.sdata.sense.SenseFactory;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseProxyFetcher extends SdataFetcher {
	protected SenseCrawlItem word = null;
	protected static CrawlItemQueue crawlItemQueue = null;

	@Override
	public void taskInitialize(){
		crawlItemQueue = CrawlItemQueue.getInstance();
	}
	
	public SenseProxyFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
	}
	
	protected SenseCrawlItem getNextItem() {
		Map<String, Object> map = crawlItemQueue.get();
		SenseCrawlItem citem = new SenseCrawlItem(map); 
		return citem;
	}
	
	@Override
	public void fetchDatumList(FetchDispatch dispatch) {
		// get item 
		SenseCrawlItem item = this.getNextItem();
		try{
	        String crawlId = item.getCrawlerName();
	        if(StringUtils.isEmpty(crawlId)){
	        	return;
	        }
	        //process item
			SenseFetcher fetcher = SenseFactory.getFetcher(crawlId);
			if(fetcher == null){
				return;
			}
			//  -- monitor start
			SenseItemMonitor.monitor(item, fetcher);
			
			// fetch list and dispatch
			fetcher.fetchDatumList(dispatch, item);
	        
	        // complete item
	        this.completeItem(item);

		}catch(Exception e){
			this.failItem(item);
			e.printStackTrace();
//			CrawlerMail.send(item.toString(),getExceptionStr(e));
		}finally{
			//--- monitor end
	        SenseItemMonitor.finish(item);
		}
	}
	
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		try{
			SenseFetchDatum senseDatum =  (SenseFetchDatum)datum;
			SenseFetcher fetcher = SenseFactory.getFetcher(senseDatum.getCrawlItem().getCrawlerName());
			SenseFetchDatum fetchDatum = fetcher.fetchDatum(senseDatum);
			//data entity check 
			if(fetchDatum == null||!fetchDatum.prepare()){
				return null;
			}
			// data range check if not right notify fetchList complete
			if(!fetchDatum.valid()){
				// notify fetch list complete
				SenseItemMonitor.notify(fetchDatum.getCrawlItem());
				return null;
			}
			return fetchDatum;
		}catch(Exception e){
			e.printStackTrace();
			//CrawlerMail.send(getExceptionStr(e));
		}
		return null;
	}
	
	private String getExceptionStr(Exception e){
		// email exception info
		StringWriter writer = new StringWriter();
		PrintWriter out = new PrintWriter(writer);
		e.printStackTrace(out);
		String result = writer.toString();
		try {
			writer.close();
			out.close();
		} catch (IOException e1) {
			
		}
		return result;
	}
	private void completeItem(SenseCrawlItem item){
		 crawlItemQueue.complete(item);
	}

	private void failItem(SenseCrawlItem item){
		 crawlItemQueue.fail(item);
	}

	@Override
	public boolean isComplete() {
		return crawlItemQueue.isComplete();
	}
	
	@Override
	public void taskFinish(){
		crawlItemQueue.reset();
	}

}
