package com.sdata.proxy.fetcher;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.proxy.item.SenseCrawlItem;

/**
 * @author zhufb
 *
 */
public class SenseItemMonitor {
	
	protected static Logger log = LoggerFactory.getLogger("Sense.SenseItemMonitor");
	
	private static Map<Long,SenseFetcher> map = new ConcurrentHashMap<Long,SenseFetcher>();
	
	/**
	 * 添加 一个item和fetcher的监控
	 * 
	 * @param item
	 * @param fetcher
	 */
	public static void monitor(SenseCrawlItem item,SenseFetcher fetcher){
		map.put(item.getId(), fetcher);
	}

	/**
	 * 通知fetcher结束这个item的执行
	 * 
	 * @param item
	 */
	public static void notify(SenseCrawlItem item){
		if(map.containsKey(item.getId())){
			SenseFetcher fetcher = map.get(item.getId());
			if(fetcher!=null&&!fetcher.isComplete(item)){
				log.warn("notify item complete "+item.getId()+ ","+item.getParamStr());
				fetcher.complete(item);
			}
		}
	}
	
	/**
	 * 结束这个item的监控
	 * 
	 * @param item
	 */
	public static void finish(SenseCrawlItem item){
		if(map!=null&&item!=null&&item.getId()!=null&&map.containsKey(item.getId())){
			map.remove(item.getId());
		}
	}
	
}
