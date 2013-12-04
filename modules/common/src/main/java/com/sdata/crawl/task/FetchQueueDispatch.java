package com.sdata.crawl.task;

import java.util.List;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.FetchDispatch;

public class FetchQueueDispatch implements FetchDispatch  {
	
	private FetchQueue queue;
	private int thresholdSize;
	private int threadWaitTimeIfNoQueue;
	private RunState state;
	
	public FetchQueueDispatch(Configuration conf,RunState state, FetchQueue queue){
		this.state = state;
		this.queue = queue;
		thresholdSize = conf.getInt("QueueThresholdSizeSlow",500);
		threadWaitTimeIfNoQueue = conf.getInt("ThreadWaitTimeIfNoQueue",1000);
	}

	public void dispatch(FetchDatum data) {
		boolean offer = queue.offer(data);
		while(!offer){
			waitfor(2000);
			offer=queue.offer(data);
		}
		this.state.setQueueSize(queue.getSize());
		if(queue.getSize()>thresholdSize){
			waitfor(2000);
		}
	}

	private void waitfor(long time) {
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}
	
	
	public boolean dispatch(List<FetchDatum> list){
		if(list!=null){
			while(true){
				if(list!=null && list.size()>0){
					FetchDatum fetchDatum = list.get(0);
					this.dispatch(fetchDatum);
					list.remove(0);
				}else{
					break;
				}
			}
		}
		return true;
	}

	public FetchDatum poll(){
		while(true){
			if(queue.getSize()>0){
				FetchDatum datum = queue.poll();
				this.state.setQueueSize(queue.getSize());
				return datum;
			}
			if(state!=null && !state.isStart()){
				return null;
			}
			waitfor(threadWaitTimeIfNoQueue);
		}
	}
	
	public long getSize(){
		return queue.getSize();
	}
}
