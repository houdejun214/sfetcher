package com.sdata.sense.fetcher;

import java.util.Map;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.sense.SenseItemQueue;
import com.sdata.sense.SenseFactory;
import com.sdata.sense.SenseFetchDatum;
import com.sdata.sense.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public abstract class SenseProxyFetcher extends SdataFetcher {
	protected SenseCrawlItem item = null;
	protected static Object syn = new Object();
	private static SenseItemQueue crawlItemQueue;

	@Override
	public void taskInitialize(){
		if(crawlItemQueue == null){
			synchronized(syn){
				if(crawlItemQueue == null){
					crawlItemQueue = this.initItemQueue();
				}
			}
		}
	}
	
	protected abstract SenseItemQueue initItemQueue();
	
	protected abstract SenseCrawlItem initItem(Map<String, Object> map);

	public SenseProxyFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
	}
	
	protected SenseCrawlItem getNextItem() {
		return initItem(crawlItemQueue.get());
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
	        // process item
			SenseFetcher fetcher = SenseFactory.getFetcher(crawlId);
			if(fetcher == null){
				return;
			}
			// monitor start
			SenseItemMonitor.monitor(item, fetcher);
			
			// fetch list and dispatch
			fetcher.fetchDatumList(dispatch, item);
	        
	        // complete item
	        this.completeItem(item);

		}catch(Exception e){
			e.printStackTrace();
			this.failItem(item);
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
	
//	private String getExceptionStr(Exception e){
//		// email exception info
//		StringWriter writer = new StringWriter();
//		PrintWriter out = new PrintWriter(writer);
//		e.printStackTrace(out);
//		String result = writer.toString();
//		try {
//			writer.close();
//			out.close();
//		} catch (IOException e1) {
//			
//		}
//		return result;
//	}
	protected void completeItem(SenseCrawlItem item){
		 crawlItemQueue.complete(item);
	}

	protected void failItem(SenseCrawlItem item){
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
