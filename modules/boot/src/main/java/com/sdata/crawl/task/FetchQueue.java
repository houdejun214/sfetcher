package com.sdata.crawl.task;

import java.util.concurrent.ArrayBlockingQueue;

import com.sdata.context.config.Configuration;
import com.sdata.core.FetchDatum;

/**
 * class of fetch datum queue.
 * the operation is thread-safe.
 * 
 * @author houdj
 *
 */
public class FetchQueue {

    public FetchQueue() {

    }

	public FetchQueue(Configuration conf) {
		int maxQueueSize = conf.getInt("MaxQueueSize", 1000);
		this.queue = new ArrayBlockingQueue<FetchDatum>(maxQueueSize);
	}
	
	private ArrayBlockingQueue<FetchDatum> queue = null;
	
	protected Object synchronizeObject=new Object();

	/**
	 * Inserts an image object at the tail of this queue if it is
     * possible to do so immediately without exceeding the queue's capacity,
     * returning <tt>true</tt> upon success and <tt>false</tt> if this queue
     * is full
     * 
	 * @param image
	 * @return
	 */
	public boolean offerOne(FetchDatum data){
		synchronized (synchronizeObject) {
			return queue.offer(data);
		}
	}
	
	/**
	 * Retrieves and removes the head of this queue,
     * or returns <tt>null</tt> if this queue is empty.
     * 
	 * @return
	 */
	public FetchDatum pollOne(){
		synchronized (synchronizeObject) {
			return queue.poll();
		}
	}
	
	public int getQueueSize(){
		synchronized (synchronizeObject) {
			return queue.size();
		}
	}
	
	public boolean offer(FetchDatum data){
		return this.offerOne(data);
	}

	public FetchDatum poll(){
		return this.pollOne();
	}
	
	public long getSize(){
		return this.getQueueSize();
	}
}
