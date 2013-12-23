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
	
	private BlockingQueue<CommonLink> queue;
	private HashSet<CommonLink> set = new HashSet<CommonLink>();
	
	public CommonLinkQueue(){
		this.queue = new LinkedBlockingQueue<CommonLink>();
	}
	
	public void add(String link,int level){
		CommonLink clink = new CommonLink(link,level);
		if(!set.contains(clink)){
			this.queue.add(clink);
			this.set.add(clink);
		}
	}
	
	public void add(List<String> links,int level){
		for(String link:links){
			this.add(link, level);
		}
	}
	
	public CommonLink get() throws InterruptedException{
		return queue.poll(2, TimeUnit.SECONDS);
	}
	
	public int size(){
		return queue.size();
	}
	
	public void clear(){
		this.set.clear();
		this.queue.clear();
	}
}
