package com.sdata.proxy;

import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.lakeside.data.redis.DistributeLock;
import com.lakeside.data.redis.DistributeLock.DistributelLockTimeOutException;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.model.QueueStatus;
import com.sdata.context.state.crawldb.CrawlDBRedis;
import com.sdata.core.item.CrawlItem;
import com.sdata.core.item.CrawlItemDB;

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
	private static Object syn = new Object(); 
	private final int WAIT_SECONDS = 5;
	private int TOTAL_WAIT_SECONDS = 300;
	
	public CrawlItemQueue(CrawlItemDB crawlItemDB){
		this.crawlItemDB = crawlItemDB;
		this.COUNT = CrawlAppContext.conf.getInt("crawler.item.queue.onetime.count", 20);
		this.TOTAL_WAIT_SECONDS = CrawlAppContext.conf.getInt("crawler.queue.wait.timeout", 300);
		this.queue = new LinkedBlockingQueue<Map<String,Object>>(COUNT*5);
		String namespace = CrawlAppContext.conf.get("crawler.item.redis.namespace","Crawl_Item");
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
