package com.sdata.crawl.task;

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import com.sdata.conf.sites.CrawlConfig;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDispatch;
import com.sdata.core.data.store.SdataStorer;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.filter.SdataFilter;

/**
 * this is the main entry task of the crawler,
 * it will startup the sub task threads for fetching
 * 
 * @author houdejun
 *
 */
public class CrawlTask extends AbstractTask {
	
	private Configuration conf;
	private CrawlConfig site;
	private SdataFetcher fetcher;
	private SdataStorer storer;
	private FetchFilter filter;
	private FetchDispatch dispatch;
	private int fetchThreadNum=0;
	private int mainCrawlThreadNum=0;
	private int taskInterval;
	private int taskPeriod;
	private CountDownLatch latch = null; 
	
	public CrawlTask(CrawlConfig site,RunState state){
		this.site = site;
		this.conf = site.getConf();
		this.state = state;
		init();
	}
	
	private void init(){
		// init the queue
		FetchQueue queue = new FetchQueue(this.conf);
		dispatch = new FetchQueueDispatch(this.conf,this.state,queue);
		// init filters
		initFilters();
		// init configuration value;
		fetchThreadNum = this.conf.getInt("FetchThreadNum", 5);
		taskInterval = this.conf.getInt("TaskInterval", 0);
		taskPeriod = this.conf.getInt("TaskPeriod", 0);
		mainCrawlThreadNum = this.conf.getInt("MainCrawlThreadNum", 1);
	}

	private void initFetcher() {
		// init the parser,resolver,storer object
		this.fetcher = (SdataFetcher)newInstance(site.getFetcherType());
		this.storer = (SdataStorer)newInstance(site.getStorer());
		this.fetcher.setStorer(storer);
	}
	
	private void initFilters(){
		List<SdataFilter> beforeFilters = new ArrayList<SdataFilter>();
		List<SdataFilter> afterFilters = new ArrayList<SdataFilter>();
		// load before filters
		List<String> filters = site.getFilters("before");
		if(filters!=null){
			for(String classType:filters){
				beforeFilters.add((SdataFilter)newInstance(classType));
			}
		}
		// load after filters
		filters = site.getFilters("after");
		if(filters!=null){
			for(String classType:filters){
				afterFilters.add((SdataFilter)newInstance(classType));
			}
		}
		this.filter = new FetchFilter(beforeFilters,afterFilters);
	}

	/**
	 * this method is used to start the whole crawl task.
	 * @throws InterruptedException 
	 */
	public void startCrawl() throws InterruptedException{
		Boolean isFetcherSingleton = this.conf.getBoolean("isFetcherSingleton",true);
		while(true){
			long taskStartTime = System.currentTimeMillis();
			subThreadList.clear();
			this.latch = new CountDownLatch(mainCrawlThreadNum);
			if(isFetcherSingleton){
				//fetcher run under singleton model
				this.initFetcher();
				fetcher.taskInitialize();
				log.info("task initialized!");
				// 1. start fetch task
				for(int i=0;i<fetchThreadNum;i++){
					TaskThread thread = new TaskThread(new FetchDatumTask(this.conf,this.state,this.dispatch,this.fetcher,this.storer,this.filter));
					subThreadList.add(thread);
					thread.start();
				}
				// 2.start crawl task
				for(int i=0;i<mainCrawlThreadNum;i++){
					TaskThread thread = new TaskThread(new FetchDatumListTask(this.conf,this.state,this.dispatch,this.fetcher,this.latch));
					subThreadList.add(thread);
					thread.start();
				}
			}else{
				//fetcher run under Multi-Instance model
				// 1. start fetch task
				log.info("crawl task running under multiple instances (fetcher) model.");
				for(int i=0;i<fetchThreadNum;i++){
					this.initFetcher();
					fetcher.taskInitialize();
					TaskThread thread = new TaskThread(new FetchDatumTask(this.conf,this.state,this.dispatch,this.fetcher,this.storer,this.filter));
					subThreadList.add(thread);
					thread.start();
				}
				// 2.start crawl task
				for(int i=0;i<mainCrawlThreadNum;i++){
					this.initFetcher();
					TaskThread thread = new TaskThread(new FetchDatumListTask(this.conf,this.state,this.dispatch,this.fetcher,this.latch));
					subThreadList.add(thread);
					thread.start();
				}
			}
			
			// wait the fetchDatumList task complete.
			latch.await();
			this.stop();
			//--wait the other threads(FetchDatum Task) stop--------------------------------
			while(true){
				try{
					Thread.sleep(5000);
					boolean isDone = true;
					for(TaskThread thread : subThreadList){
						if(!thread.isDone()){
							isDone = false;
							break;
						}
					}
					if(isDone){
						break;
					}
				}
				catch(Exception e){
					e.printStackTrace();
				}
			}
			
			//---------------------------------------------------------------------------
			this.updateState();
			if(taskInterval<=0 && taskPeriod<=0){
				break;
			}
			log.info("current crawl task is finished.");
			if(isFetcherSingleton){
				fetcher.taskFinish();
			}else{
				for(TaskThread thread:subThreadList){
					AbstractTask task = thread.getTask();
					if(task instanceof FetcherBase){
						FetcherBase fetcherTask = (FetcherBase) task;
						SdataFetcher _fetcher = fetcherTask.getFetcher();
						if(_fetcher!=null){
							_fetcher.taskFinish();
						}
					}
				}
			}
			// check if need to restart the task.
			long now = System.currentTimeMillis();
			long wait = taskInterval;
			if(taskPeriod>0){
				long taskduration = now - taskStartTime;
				wait = taskPeriod - taskduration;
				if(wait<0){
					wait = 0;
				}
			}
			log.info("current crawl task is completed overall with finish operation and wait {} millis to start next task! ",wait);
			log.info("*****************************************************************");
			this.await(wait);
		}
		log.info("crawl task is completed");
	}
	
	/**
	 * add application shutdown hook event handler
	 * this will be raised when kill or ctrl+C；
	 * 
	 */
	public void addShutdownHook(){
		final CrawlTask crawlTask = this;
		Runtime.getRuntime().addShutdownHook(new Thread() {  
            public void run() {  
            	// wait the fetchDatumList task complete.
    			try {
					latch.await();
				} catch (InterruptedException e1) {
					e1.printStackTrace();
				}
    			crawlTask.stop();
    			//--wait the other threads(FetchDatum Task) stop--------------------------------
    			while(true){
    				try{
    					Thread.sleep(5000);
    					boolean isDone = true;
    					for(TaskThread thread : subThreadList){
    						if(!thread.isDone()){
    							isDone = false;
    							break;
    						}
    					}
    					if(isDone){
    						break;
    					}
    				}
    				catch(Exception e){
    					e.printStackTrace();
    				}
    			}
    			
    			//---------------------------------------------------------------------------
    			crawlTask.updateState();
    			log.info("crawl task was stopped");
            }  
        });  
	}
	
	public long getDispathQueueSize(){
		if(dispatch!=null){
			return dispatch.getSize();
		}
		return 0;
	}

	private void updateState() {
//		this.state.addCycle();
	}
	
	/**
	 * create new instance 
	 * @param classType
	 * @return
	 */
	private Object newInstance(String classType) {
		try {
			Class<?> type = Class.forName(classType);
			try {
				Constructor<?> constructor = type.getConstructor(Configuration.class,RunState.class);
				return constructor.newInstance(this.conf,this.state);
			} catch (NoSuchMethodException e) {
				Constructor<?> constructor = type.getConstructor(Configuration.class);
				return constructor.newInstance(this.conf);
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException(e.getMessage());
		}
	}

	@Override
	public String getTaskName() {
		return "MainCrawlTask";
	}
	
	@Override
	public void run() {
	}
}
