package com.sdata.crawl.task;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TaskThread extends Thread {
	
	protected final AbstractTask parentTask ;
	
	protected Logger log = LoggerFactory.getLogger("SdataCrawler.TaskThread");
	
	private final AbstractTask task ;
	
	private static int taskInitNumber;
	
	private boolean isdone = false;
	
    private static synchronized int nextTaskNum() {
    	return taskInitNumber++;
    }
	
	public AbstractTask getTask() {
		return task;
	}

	public TaskThread( AbstractTask parentTask,AbstractTask target){
		super(target);
		this.task = target;
		this.parentTask = parentTask;
		//this.setDaemon(true);
		this.setUncaughtExceptionHandler(new UncaughtExceptionHandler() {
			public void uncaughtException(Thread t, Throwable e) {
				System.out.print("thread"+t.getId()+" process failed:");
				e.printStackTrace();
			}
		});
	}
	
	public TaskThread(AbstractTask target){
		super(target);
		this.task = target;
		this.setName(target.getTaskName()+"-"+nextTaskNum());
		this.parentTask = null;
	}
	
	public Long getTaskId(){
		if(task!=null){
			return task.getTaskId();
		}
		return null;
	}

	@Override
	public void run() {
		try{
			log.info("thread [{}] started",this.getName());
			if(this.task!=null){
				task.start();
			}
			super.run();
		}finally{
			isdone = true;
			log.info("thread [{}] stopped",this.getName());
			//stopThreadTask();
		}
	}
	
	public boolean isDone(){
		return isdone;
	}
	
	/**
	 * stop the task that bind to the thread
	 */
	public void stopThreadTask() {
		if(this.task!=null && !this.task.getStop()){
			task.stop();
		}
	}
}
