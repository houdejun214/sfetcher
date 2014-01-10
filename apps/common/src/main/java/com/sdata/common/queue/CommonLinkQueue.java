package com.sdata.common.queue;

import java.util.HashSet;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * @author zhufb
 *
 */
public class CommonLinkQueue{
	
	private int DefaultMaxSize = 5000;
	private int DefaultWaiting = 2;
	private BlockingQueue<CommonLink> queue;
	private HashSet<CommonLink> set = new HashSet<CommonLink>();
	private Boolean empty = false;
	
	public CommonLinkQueue(){
		this.queue = new LinkedBlockingQueue<CommonLink>();
	}
	
	public void add(String link,int level){
		CommonLink clink = new CommonLink(link,level);
		if(!set.contains(clink)){
//			if(size() > DefaultMaxSize){
//				this.await(DefaultWaiting);
//			}
			synchronized(empty){
				if(!empty){
					this.queue.offer(clink);
					this.set.add(clink);
				}
			}
		}
	}
	
	private void await(long time) {
		try {
			Thread.sleep(time*1000);
		} catch (InterruptedException e) {
		}
	}
	
	public void add(List<String> links,int level){
		for(String link:links){
			this.add(link, level);
		}
	}
	
	public CommonLink get() throws InterruptedException{
		return queue.poll(DefaultWaiting, TimeUnit.SECONDS);
	}
	
	public int size(){
		return queue.size();
	}
	
	public void clear(){
		synchronized(empty){
			this.set.clear();
			this.queue.clear();
			empty = true;
		}
	}
}
