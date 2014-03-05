package com.sdata.crawl.task;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.context.state.RunState;


public abstract class AbstractTask implements Runnable{

	protected Logger log = LoggerFactory.getLogger("SdataCrawler."+this.getClass().getSimpleName());
	protected Long taskId;
	protected RunState state;
	protected Boolean stop = true;
	private Object synchrObject=new Object();
	protected final List<TaskThread> subThreadList = new ArrayList<TaskThread>();
	public Long getTaskId() {
		return taskId;
	}

	public void setTaskId(Long taskId) {
		this.taskId = taskId;
	}

	public RunState getState() {
		return state;
	}

	public void setState(RunState state) {
		this.state = state;
	}

	public Boolean getStop() {
		return stop;
	}

	public Object getSynchrObject() {
		return synchrObject;
	}
	
	public abstract String getTaskName();
	
	public abstract void run();
	
	public void start(){
		this.stop=false;
		if(this.state!=null && !this.state.isStart()){
			this.state.setStart(true);
			synchronized (this.state) {
				this.saveState();
			}
		}
	}
	
	public void stop(){
		if(this.state!=null){
			this.state.setStart(false);
		}
		if(!this.stop){
			this.stop = true;
			this.saveState();
		}
		// stop sub thread
		for(TaskThread thread:subThreadList){
			thread.stopThreadTask();
		}
	}
	
	public void saveState(){}
	
	protected void await(long time){
		try {
			Thread.sleep(time);
		} catch (InterruptedException e) {
			log.info(e.getMessage());
		}
	}
	
	public Map<String,String> getThreadState(){
		Map<String,String> map = new HashMap<String,String>();
		for(TaskThread thread:subThreadList){
			String name = thread.getName();
			String state = thread.getState().toString();
			map.put(name, state);
		}
		return map;
	}
}
