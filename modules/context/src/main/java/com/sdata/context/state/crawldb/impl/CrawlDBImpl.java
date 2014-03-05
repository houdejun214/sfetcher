package com.sdata.context.state.crawldb.impl;

import java.util.List;
import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.state.CrawlDBRunState;
import com.sdata.context.state.crawldb.CrawlDB;
import com.sdata.context.state.crawldb.CrawlDBImageQueue;
import com.sdata.context.state.crawldb.CrawlDBQueue;

public class CrawlDBImpl implements CrawlDB{
	
	private CrawlDBRunState dbRunState=null;
	private CrawlDBQueue dbQueue=null;
	private CrawlDBImageQueue dbimageQueue = null;
	private Configuration conf = null;
	
	public CrawlDBImpl(Configuration conf){
		this.conf = conf;
		dbRunState = new CrawlDBRunStateImpl(conf);
	}
	
	/*******************************************************/
	/*********** 下面是 CrawlDBRunState 接口实现************/
	public Map<String, String> queryAllRunState() {
		return dbRunState.queryAllRunState();
	}

	public String getRunState(String key) {
		return dbRunState.getRunState(key);
	}

	public Boolean updateRunState(String key, String val) {
		return dbRunState.updateRunState(key, val);
	}

	
	/*******************************************************/
	/************* 下面是 CrawlDBQueue 接口实现************/
	
	public List<Map<String, Object>> queryQueue(int topN) {
		this.ensureDbQueue();
		return dbQueue.queryQueue(topN);
	}

	public List<Map<String, Object>> queryQueueByStatus(int topN, String status) {
		this.ensureDbQueue();
		return dbQueue.queryQueueByStatus(topN, status);
	}

	public List<Map<String, Object>> queryQueue(String tableName, int topN,int lastId) {
		this.ensureDbQueue();
		return dbQueue.queryQueue(tableName, topN, lastId);
	}

	public int queryQueueTotoalCount() {
		this.ensureDbQueue();
		return dbQueue.queryQueueTotoalCount();
	}

	public Boolean updateQueueComplete(String key) {
		this.ensureDbQueue();
		return dbQueue.updateQueueComplete(key);
	}

	public Boolean updateQueueStatus(String key, String status) {
		this.ensureDbQueue();
		return dbQueue.updateQueueStatus(key, status);
	}

	public Boolean changeQueueStatus(String oldStatus, String newStatus) {
		this.ensureDbQueue();
		return dbQueue.changeQueueStatus(oldStatus, newStatus);
	}

	public Boolean insertQueueObjects(List<Map<String, Object>> list) {
		this.ensureDbQueue();
		return dbQueue.insertQueueObjects(list);
	}

	public Boolean isQueueDepthComplete(String depth) {
		this.ensureDbQueue();
		return dbQueue.isQueueDepthComplete(depth);
	}

	public Boolean deleteQueueByKey(String key) {
		this.ensureDbQueue();
		return dbQueue.deleteQueueByKey(key);
	}

	public void deleteQueue() {
		this.ensureDbQueue();
		dbQueue.deleteQueue();
	}

	public void resetQueueStatus() {
		this.ensureDbQueue();
		dbQueue.resetQueueStatus();
	}
	
	/**
	 * 确保只有在使用的时候才创建DBQueue
	 */
	private void ensureDbQueue(){
		if(dbQueue == null){
			synchronized(this){
				if(dbQueue == null){
					dbQueue = new CrawlDBQueueImpl(conf);
				}
			}
		}
	}
	
	/*******************************************************/
	/************* 下面是 CrawlDBImageQueue 接口实现************/
	public List<Map<String,Object>> queryImageQueue(int count) {
		this.ensureDbImageQueue();
		return dbimageQueue.queryImageQueue(count);
	}

	public Boolean isImageExists(String url, String source) {
		this.ensureDbImageQueue();
		return dbimageQueue.isImageExists(url, source);
	}

	public void insertImageQueue(List<String> list, String source) {
		this.ensureDbImageQueue();
		dbimageQueue.insertImageQueue(list, source);
		
	}

	public void updateImageQueue(String id, String status) {
		this.ensureDbImageQueue();
		dbimageQueue.updateImageQueue(id, status);
	}

	public Map<String, Object> getOneImage() {
		this.ensureDbImageQueue();
		return dbimageQueue.getOneImage();
	}

	/**
	 * 确保只有在使用的时候才创建DBImageQueue
	 */
	private void ensureDbImageQueue(){
		if(dbimageQueue == null){
			synchronized(this){
				if(dbimageQueue == null){
					dbimageQueue = new CrawlDBImageQueueImpl(conf);
				}
			}
		}
	}

}
