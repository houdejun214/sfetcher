package com.sdata.crawl.task;

import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;
import com.sdata.core.exception.NegligibleException;
import com.sdata.core.fetcher.SdataFetcher;

/**
 * 
 * 
 * @author houdejun
 *
 */
public class FetchDatumListTask extends AbstractTask implements Runnable,FetcherBase {
	
	private FetchDispatch dispatch ;
	
	private SdataFetcher fetcher;
	
	private int fetchSleepInterval;
	
	private CountDownLatch latch;
	
	private String specifiedStartTime;
	private String specifiedEndTime;
	
	public FetchDatumListTask(Configuration conf, RunState state,
			FetchDispatch dispatch, SdataFetcher fetcher, CountDownLatch latch) {
		this.dispatch = dispatch;
		this.fetcher = fetcher;
		this.state = state;
		fetchSleepInterval = conf.getInt("FetchSleepInterval", 0);
		this.latch = latch;
		specifiedStartTime = conf.get("specifiedStartTime", "00:00:00");
		specifiedEndTime = conf.get("specifiedEndTime", "23:59:59");
	}

	public void run() {
		try{
			// 1. start to fetch list;
			while(!stop){
				long startTime = System.currentTimeMillis();
				List<FetchDatum> datums=null;
				try {
					while(true){
						Date now = new Date();
						Date begin =this.genDate(specifiedStartTime);
						Date end = this.genDate(specifiedEndTime);
						if(now.after(begin) && now.before(end)){
							break;
						}else{
							this.await(10000);
							log.info("crawl task wait for specified time to begin.");
							continue;
						}
					}
					fetcher.fetchDatumList(dispatch);
					datums = fetcher.fetchDatumList();
				} catch (NegligibleException ne) {
					// don't need to stop when this exception raised.
					log.warn(ne.getMessage(),ne);
				} catch (RuntimeException re) {
					// runtime exception is thrown by our self, it must be some fatal error in program. so we need stop crawl.
					//this.stop();
					throw re; 
				} catch (Exception e){
					log.error(e.getMessage(),e);
				}
				dispatch.dispatch(datums);
				if(fetcher.isComplete()){
					return;
					//this.stop();
				}else if(fetchSleepInterval>0){
					try {
						long curTime = System.currentTimeMillis();
						long curSleep = fetchSleepInterval - (curTime - startTime);
						if(curSleep>0 || startTime<=0){
							Thread.sleep(curSleep);
						}
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}finally{
			latch.countDown();
		}
	}
	
	@Override
	public String getTaskName() {
		return "FetchDatumList";
	}
	
	private Date genDate(String specifiedTime){
		String[] time = specifiedTime.split(":");
		if(time.length!=3){
			throw new RuntimeException("Incorrect specified time format.");
		}
		int hour = Integer.parseInt(time[0]);
		int minute = Integer.parseInt(time[1]);
		int second = Integer.parseInt(time[2]);
		Date date = new Date();
		date.setHours(hour);
		date.setMinutes(minute);
		date.setSeconds(second);
		return date;
	}
	
	public SdataFetcher getFetcher(){
		return this.fetcher;
	}
}
