package com.sdata.crawl.task;

import com.lakeside.core.utils.StringUtils;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDispatch;
import com.sdata.core.FetchDatum;
import com.sdata.core.NegligibleException;
import com.sdata.core.RunState;
import com.sdata.core.data.SdataStorer;
import com.sdata.core.fetcher.SdataFetcher;

/**
 * 
 * @author houdj
 *
 */
public class FetchDatumTask extends AbstractTask implements Runnable, FetcherBase {
	
	protected FetchDispatch dispatch ;
	
	protected SdataFetcher fetcher;
	
	protected SdataStorer storer;

	protected FetchFilter filter;
	
	private int threadWaitTimeIfNoQueue = 1000;
	
	public FetchDatumTask(Configuration conf,RunState state, FetchDispatch dispatch,SdataFetcher fetcher, SdataStorer storer,FetchFilter filter){
		this.filter = filter;
		this.dispatch = dispatch;
		this.fetcher = fetcher;
		this.storer = storer;
		this.state = state;
		this.threadWaitTimeIfNoQueue = conf.getInt("ThreadWaitTimeIfNoQueue",1000);
	}
	
	/**
	 * running method
	 */
	public void run() {
		// task will stop executing when task is stop and without exists queue crawl datum. 
		while(!stop || dispatch.getSize()>0){
			FetchDatum data = dispatch.poll();
			if(data!=null){
				if(filter.filterBeforeFetch(data)){
					try {
						data = fetchOne(data);
						if(data==null){
							//
							continue;
						}
						saveOne(data);
						finishOne(data);
						if(StringUtils.isNotEmpty(data.getCurrent())){
							this.state.updateCurrentFetchState(data.getCurrent());
						}
					}catch (NegligibleException ee){
						logWarnMessage(data,ee);
						this.state.addOneFailed();
					}catch(RuntimeException re){ 
						logErrorMessage(data,re);
						this.state.addOneFailed();
					}catch (Exception e) {
						logErrorMessage(data,e);
						this.state.addOneFailed();
					}
				}
			}else{
				// sleep current thread for some time if there is no queued data by now,
				try {
					Thread.sleep(threadWaitTimeIfNoQueue);
				} catch (InterruptedException e) {
					log.info(e.getMessage());
				}
			}
			stop = (!this.state.isStart());
		}
		//log.info("stop {}, dispatch size {}\n",stop,dispatch.getSize());
	}

	private void saveOne(FetchDatum data) throws Exception {
		if(storer!=null){
			storer.save(data);
		}
	}
	
	private void finishOne(FetchDatum data) {
		state.addOneSuccess();
		try {
			fetcher.datumFinish(data);
		} catch (Exception e) {
			log.warn("error when datum finish "+e.getMessage(),e);
		}
	}

	/**
	 * process one image object
	 * @param data
	 * @return 
	 * @throws Exception 
	 */
	protected FetchDatum fetchOne(FetchDatum datum) {
		return fetcher.fetchDatum(datum);
	}
	
	protected void logFaileMessage(FetchDatum data,String message){
		String msg = "data fetch failed : "+message;
		msg+= "\r\n at fetch "+data.getUrl();
		log.error(msg);
	}
	
	protected void logWarnMessage(FetchDatum data, Exception e){
		String msg = "data fetch failed : "+e.getMessage() +" at fetch ["+data.getUrl()+"]";
		log.warn(msg);
	}
	
	protected void logErrorMessage(FetchDatum data, Exception e){
		String msg = "data fetch failed : "+e.getMessage();
		msg+= "\r\n at fetch ["+data.getUrl()+"] current ["+data.getCurrent()+"]";
		Throwable cause = e.getCause();
		if(cause!=null){
			msg+="\r\n cause by :"+cause.getClass()+"\n"+cause.getMessage();
		}
		log.error(msg,e);
	}

	@Override
	public String getTaskName() {
		return "FetchDatum";
	}
	
	public SdataFetcher getFetcher(){
		return this.fetcher;
	}
}
