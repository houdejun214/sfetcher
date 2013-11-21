package com.sdata.index;

import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import com.mongodb.DBObject;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.RunState;

/**
 * @author zhufb
 *
 */
public class DBDataQueue {
	private BlockingQueue<DBObject> queue;
	private DBDataDao dbData; 
	private final int COUNT = 1000; 
	private RunState state;
	private Date maxFetchTime = new Date(2000,01,01);
	private long maxId = 0;
	private int indexCount = 0;
	public DBDataQueue(DBDataDao dbData){
		this.queue = new LinkedBlockingQueue<DBObject>(COUNT*5);
		this.dbData = dbData;
		this.state = CrawlAppContext.state;
		this.maxId = state.getIndexMaxId();
		this.indexCount = state.getIndexCount();
		//this.add();
		//UUID id index add method
		this.addNatural();
		//tencent use fetchTime
//		this.addFetchTime();
	}
	
	public DBObject get(){
		this.check();
		return queue.poll();
	}
	
	private void add(){
		state.setIndexMaxId(maxId);
		List<DBObject> list = dbData.query(COUNT,maxId);
		if(list == null||list.size() == 0) {
			return;
		}
		queue.addAll(list);
		maxId = Long.valueOf(list.get(list.size()-1).get(Constants.OBJECT_ID).toString());
	}
	
	private void addFetchTime(){
		List<DBObject> list = dbData.query(COUNT,maxFetchTime);
		if(list == null||list.size() == 0) {
			return;
		}
		queue.addAll(list);
		maxFetchTime = (Date)list.get(list.size()-1).get(Constants.FETCH_TIME);
	}

	private void addNatural(){
		List<DBObject> list = dbData.query(COUNT, indexCount, new Date());
		if(list == null||list.size() == 0) {
			return;
		}
		queue.addAll(list);
		indexCount += list.size();
	}
	
	private void check(){
		if(size() > 0) return;
		synchronized (this) {
			if(size() > 0) return;
//			this.add();
			this.addNatural();
		}
		if(size() == 0){
			DBIndexTask.setComplete(true);
			return;
		}
	}
	
	private int size(){
		return queue.size();
	}
}
