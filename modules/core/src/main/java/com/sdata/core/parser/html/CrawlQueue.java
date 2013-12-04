package com.sdata.core.parser.html;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.data.redis.DistributeLock;
import com.lakeside.data.redis.DistributeLock.DistributelLockTimeOutException;
import com.sdata.context.config.Constants;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.model.QueueStatus;
import com.sdata.context.state.crawldb.CrawlDB;
import com.sdata.context.state.crawldb.CrawlDBRedis;
import com.sdata.core.parser.config.StrategyConfig;

/**
 * @author zhufb
 *
 */
public class CrawlQueue {
	private BlockingQueue<Map<String,Object>> queue;
	private CrawlDB crawlDB;
	private DistributeLock crawlLock;
	private final int COUNT = Constants.FETCH_COUNT; 
	private static CrawlQueue crawlQueue;
	private static Object syn = new Object(); 
	private final int WAIT_SECONDS = 30;
	private int TOTAL_WAIT_SECONDS = 300;
	
	public static CrawlQueue getInstance(){
		if(crawlQueue == null){
			synchronized (syn) {
				if(crawlQueue == null)
					crawlQueue=  new CrawlQueue();
			}
		}
		return crawlQueue;
	}
	
	private CrawlQueue(){
		this.queue = new LinkedBlockingQueue<Map<String,Object>>(COUNT*5);
		this.crawlDB = CrawlAppContext.db;
		this.crawlLock = new DistributeLock(CrawlDBRedis.getRedisDB(CrawlAppContext.conf,"Crawl_Queue"));
		this.crawlLock.setTimeout(WAIT_SECONDS);
		this.TOTAL_WAIT_SECONDS = CrawlAppContext.conf.getInt("crawler.queue.wait.timeout", 300);
		this.init();
	}
	
	private void init(){
		if(getDBQueueSize() == 0){
			try {
				crawlLock.lock();

				if(getDBQueueSize() == 0){
					this.addNextSite();
				}
				this.crawlLock.release();
			} catch (DistributelLockTimeOutException e) {
				
			}
		}
	}
	
	public Map<String,Object> get(){
		this.check();
		return queue.poll();
	}
	
	
	protected void check(){
			if(size() == 0){
				synchronized (this) {
					// add queue in this site
					int currentWaitSeconds = 0;
					while(size()  == 0 && currentWaitSeconds<TOTAL_WAIT_SECONDS) {
						// db lock
						try{
							crawlLock.lock();
							
							this.add();
							
							// waiting WAIT_SECONDS if queue.size == 0 else unlock 
							if(size() > 0){
								// unlock
								this.crawlLock.release();
							}else{
								this.crawlLock.wait(WAIT_SECONDS);
								currentWaitSeconds+=WAIT_SECONDS;
							}
						} catch (DistributelLockTimeOutException e) {
							currentWaitSeconds+=WAIT_SECONDS;
						}
					}
					//add queue in next site
					if(size() == 0) {
						// clear queue before next site crawl
						this.clear();
						// add nextSite
						this.addNextSite();
						// unlock
						this.crawlLock.release();
					}
				}
			}

		if(size() == 0){
			SdataHtmlFetcher.setComplete(true);
			return;
		}
	}

	protected void add(){
		List<Map<String, Object>> list = crawlDB.queryQueue(COUNT);
		for(Map<String,Object> map:list ){
			String key = (String)map.get(Constants.QUEUE_KEY);
			crawlDB.updateQueueStatus(key, QueueStatus.RUNING);
		}
		queue.addAll(list);
	}
	
	/**
	 * move to next site
	 */
	protected void addNextSite(){
		// init next site entry
		Map<String,Object> map = new HashMap<String,Object>();
		String nextInit = StrategyConfig.getInstance(CrawlAppContext.conf).getNextInit();
		if(nextInit == null){
			return;
		}
		map.put(Constants.QUEUE_KEY, StringUtils.md5Encode(nextInit));
		map.put(Constants.QUEUE_URL, nextInit);
		map.put(Constants.QUEUE_DEPTH, 0);
		map.put(Constants.QUEUE_STATUS, QueueStatus.RUNING);
		// put next site entry into database
		this.put(map);
		// put data in this queue
		queue.add(map);
	}
	
	public void put(List<Map<String,Object>> list) {
		if(list == null) {
			return;
		}
		
		// max queue level
		if(!checkQueueMaxLevel(list)){
			return;
		}
		// max queue size
		if(!checkQueueMaxSize()){
			return;
		}
		crawlDB.insertQueueObjects(list);
	}

	private boolean checkQueueMaxLevel(List<Map<String,Object>> list){
		Long max = CrawlAppContext.conf.getLong("crawler.queue.level.max");
		if(max == null){
			return true;
		}
		
		for(Map<String,Object> map:list){
			Integer depth = (Integer)map.get(Constants.QUEUE_DEPTH);
			if(depth>max){
				return false;
			}
		}
		return true;
	}
	
	private boolean checkQueueMaxSize(){
		Long max = CrawlAppContext.conf.getLong("crawler.queue.size.max");
		if(max == null){
			return true;
		}
		int count = getDBQueueSize();
		if(count < max ){
			return true;
		}
		return false;
	}

	private int getDBQueueSize(){
		return crawlDB.queryQueueTotoalCount();
	}

	private void put(Map<String,Object> map) {
		if(map == null) {
			return;
		}
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		list.add(map);
		this.put(list);
	}
	
	public void complete(Map<String,Object> current){
		if(current == null){
			return;
		}
		Object key = current.get(Constants.QUEUE_KEY);
		if(key == null){
			return;
		}
		crawlDB.updateQueueComplete(key.toString());
	}
	
	protected void clear(){
		crawlDB.deleteQueue();
	}
	
	protected int size(){
		return queue.size();
	}
}
