package com.sdata.core.crawldb;

import java.util.List;
import java.util.Map;

public interface CrawlDBQueue {

	public List<Map<String, Object>> queryQueue(int topN);

	public List<Map<String, Object>> queryQueueByStatus(int topN,String status);

	public List<Map<String, Object>> queryQueue(String tableName,int topN, int lastId);

	public int queryQueueTotoalCount();

	public Boolean updateQueueComplete(final String key);

	public Boolean updateQueueStatus(final String key, String status);

	public Boolean changeQueueStatus(String oldStatus, String newStatus);

	public Boolean insertQueueObjects(final List<Map<String, Object>> list);

	public Boolean isQueueDepthComplete(final String depth);

	/**
	 * delete a queue item by key
	 * @param key
	 * @return
	 */
	public Boolean deleteQueueByKey(String key);

	/**
	 * delete all queue data
	 * 
	 * @return
	 */
	public void deleteQueue();

	public void resetQueueStatus();

}