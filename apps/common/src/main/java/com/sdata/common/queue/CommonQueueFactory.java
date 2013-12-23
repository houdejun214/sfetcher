package com.sdata.common.queue;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.sdata.common.CommonItem;

/**
 * @author zhufb
 *
 */
public class CommonQueueFactory {

	private static  Map<CommonItem,CommonLinkQueue> map = new HashMap<CommonItem,CommonLinkQueue>();
	
	/**
	 * get link queue by common item 
	 * 
	 * @param item
	 * @return
	 */
	public static CommonLinkQueue getLinkQueue(CommonItem item){
		if(!map.containsKey(item)){
			Lock lock = new ReentrantLock();
			lock.lock();
			if(!map.containsKey(item)){
				map.put(item, new CommonLinkQueue());
			}
			lock.unlock();
		}
		return map.get(item);
	}
}
