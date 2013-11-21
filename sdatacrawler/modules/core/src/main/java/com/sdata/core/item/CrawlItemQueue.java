package com.sdata.core.item;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.lang.StringUtils;

import com.lakeside.data.redis.DistributeLock;
import com.lakeside.data.redis.DistributeLock.DistributelLockTimeOutException;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.QueueStatus;
import com.sdata.core.crawldb.CrawlDBRedis;

/**
 * @author zhufb
 *
 */
public class CrawlItemQueue {
	
	private BlockingQueue<Map<String,Object>> queue;
	private boolean complete = false;
	private CrawlItemDB crawlItemDB;
	private DistributeLock crawlLock;
	private final int COUNT; 
	private static CrawlItemQueue itemQueue;
	private static Object syn = new Object(); 
	private final int WAIT_SECONDS = 5;
	private int TOTAL_WAIT_SECONDS = 300;
	public static CrawlItemQueue getInstance(){
		if(itemQueue == null){
			synchronized (syn) {
				if(itemQueue == null)
					itemQueue=  new CrawlItemQueue();
			}
		}
		return itemQueue;
	}
	
	private CrawlItemQueue(){
		this.COUNT = CrawlAppContext.conf.getInt("crawler.item.queue.onetime.count", 20);
		this.TOTAL_WAIT_SECONDS = CrawlAppContext.conf.getInt("crawler.queue.wait.timeout", 300);
		this.queue = new LinkedBlockingQueue<Map<String,Object>>(COUNT*5);
		this.crawlItemDB = new CrawlItemDB(CrawlAppContext.conf);
		String namespace = CrawlAppContext.conf.get("next.crawler.item.redis.namespace");
		if(StringUtils.isEmpty(namespace)){
			namespace = "Next_Crawl_Item";
		}
		this.crawlLock = new DistributeLock(CrawlDBRedis.getRedisDB(CrawlAppContext.conf,namespace));
		this.crawlLock.setTimeout(WAIT_SECONDS);
	}
	
	public Map<String,Object> get(){
		this.check();
		return queue.poll();
	}
	
	protected void check(){
		if(!isComplete()&&size() == 0){
			synchronized (syn) {
				int currentWaitSeconds = 0;
				// add queue 
				//Get Lock
				boolean isLock = false;
				while(!isLock&&!isComplete()&&size() ==0&&currentWaitSeconds<TOTAL_WAIT_SECONDS){
					try{
						// db lock
						crawlLock.lock();
						isLock = true;
					} catch (DistributelLockTimeOutException e) {
						currentWaitSeconds+=WAIT_SECONDS;
					}
				}
				//add Data
				while(isLock&&!isComplete()&&size() ==0 &&currentWaitSeconds < TOTAL_WAIT_SECONDS) {
						//add items
						this.add();
						//
						if(size() > 0){
							currentWaitSeconds = 0;
						}else{
							this.crawlLock.wait(WAIT_SECONDS);
							currentWaitSeconds+=WAIT_SECONDS;
						}
				}
				//add queue from running not completeed
				if(isLock&&size() == 0) {
					complete = true;
					//reset running
//					this.resetRuning();
					
//					this.add();
					
//					if(size() == 0){
//					}
				}
				//unlock db
				if(isLock){
					this.crawlLock.release();
				}
			}
		}
	}

	protected void add(){
		List<Map<String, Object>> list = crawlItemDB.queryItemQueue(COUNT);
		for(Map<String,Object> map:list ){
			Long id = (Long)map.get("id");
			crawlItemDB.updateItemStatus(id, QueueStatus.RUNING);
		}
		queue.addAll(list);
	}
	
	public void complete(CrawlItem item){
		if(item == null){
			return;
		}
		Long id = item.getId();
		if(id == null){
			return;
		}
		crawlItemDB.updateItemComplete(id);
	}

	public void fail(CrawlItem item){
		if(item == null){
			return;
		}
		Long id = item.getId();
		if(id == null){
			return;
		}
		crawlItemDB.updateItemStatus(id,QueueStatus.FAILED);
	}
	
//	public void resetRuning(){
//		crawlItemDB.updateItemStatus(QueueStatus.RUNING, QueueStatus.INIT);
//	}
	
	public void reset(){
		crawlItemDB.resetItemStatus();
		complete = false;
	}
	
	protected int size(){
		return queue.size();
	}

	public boolean isComplete() {
		return complete;
	}
}
