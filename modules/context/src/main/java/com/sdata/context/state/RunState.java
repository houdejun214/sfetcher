package com.sdata.context.state;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.beanutils.BeanUtilsBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.lakeside.core.utils.time.StrToDateConverter;
import com.lakeside.core.utils.time.TimeSpan;
import com.lakeside.core.utils.time.TimeSpanConverter;
import com.sdata.context.config.Constants;

public class RunState {
	private Logger log = LoggerFactory.getLogger("SdataCrawler.RunState");
	public final static String GlobalSiteName="all";
	private long nextSequenceId = 1; 	// start from 1
	private long queueSize;
	private long indexMaxId;
	//indexed count use to skip mongodb query data 
	private  AtomicInteger indexCount = new AtomicInteger(0) ;
	// start time of the task
	private Date currentTaskStartTime;
	// end time of the task
	private Date currentTaskEndTime;
	//the endTime with the last task that is finish normal
	private Date lastTaskEndTime;
	
	private boolean isStart;

	/**
	 * 1.0.0 推荐使用参数如下
	 * 
	 * */
	private String crawlName;
	private long cycle;
	private int currentEntry = 0;
	private String currentFetchState;
	private long successCount;
	private long failedCount;
	private long repeatDiscardCount;
	private long unexpectedDiscardCount;
	private final CrawlDBRunState db;
	
	public String getCrawlName() {
		return crawlName;
	}

	public void setCrawlName(String crawlName) {
		this.crawlName = crawlName;
	}

	public long getSuccessCount() {
		return successCount;
	}

	public void setSuccessCount(long successCount) {
		this.successCount = successCount;
	}

	public long getFailedCount() {
		return failedCount;
	}

	public void setFailedCount(long failedCount) {
		this.failedCount = failedCount;
	}

	public long getRepeatDiscardCount() {
		return repeatDiscardCount;
	}

	public void setRepeatDiscardCount(long repeatDiscardCount) {
		this.repeatDiscardCount = repeatDiscardCount;
	}

	public long getQueueSize() {
		return queueSize;
	}

	public void setQueueSize(long queueSize) {
		this.queueSize = queueSize;
	}

	public String getCurrentFetchState() {
        if(currentFetchState==null){

        }
		return currentFetchState;
	}

	public void setCurrentFetchState(String currentFetchState) {
        this.updateCurrentFetchState(currentFetchState);
	}

	public long getUnexpectedDiscardCount() {
		return unexpectedDiscardCount;
	}

	public void setUnexpectedDiscardCount(long unexpectedDiscardCount) {
		this.unexpectedDiscardCount = unexpectedDiscardCount;
	}

	public long getCycle() {
		return cycle;
	}

	public void setCycle(long cycle) {
		this.cycle = cycle;
	}
	public boolean isStart() {
		return isStart;
	}

	public void setStart(boolean isStart) {
		this.isStart = isStart;
	}

	public void addOneSuccess(){
		this.successCount++;
		if(this.successCount % 50 == 0){
			try{
				this.save("successCount", this.successCount);
			}catch(Exception e){
				log.warn("add success count failed: {}", e.getMessage());
			}
		}
	}

	public void addOneRepeatDiscard(){
		this.repeatDiscardCount++;
		this.save("repeatDiscardCount", this.repeatDiscardCount);
	}
	
	public void addOneUnexpectedDiscard(){
		this.unexpectedDiscardCount++;
		this.save("unexpectedDiscardCount", this.unexpectedDiscardCount);
	}
	
	public void addOneFailed(){
		this.failedCount++;
		this.save("failedCount", this.failedCount);
	}
	
	public void addCycle(){
		this.cycle++;
		this.save(Constants.CYCLE, this.cycle);
	}

	public void addIndexCount(int count){
		int addAndGet = indexCount.addAndGet(count);
		if(addAndGet%100 == 0)
			this.saveIndexCount();
	}

	public void setIndexCount(int count){
		this.indexCount.set(count);
		this.saveIndexCount();
	}

	public void saveIndexCount(){
		this.save("indexCount", this.indexCount.intValue());
	}
	
	
	public RunState(String crawlName, CrawlDBRunState db){
		this.crawlName = crawlName;
		this.db = db;
		loadFromDB();
	}
	
	private void loadFromDB(){
		if(db==null) return;
  		Map<String, String> map = db.queryAllRunState();
		Map<String, String> curMap = db.queryAllRunState();
		map.putAll(curMap);
		try {
			BeanUtilsBean instance = BeanUtilsBean.getInstance();
			instance.getConvertUtils().register(new TimeSpanConverter(), TimeSpan.class);
			instance.getConvertUtils().register(new StrToDateConverter(), Date.class);
			instance.copyProperties(this, map);
		} catch (Exception e) {
			System.out.print(e.getMessage());
		}
	}
	
	private void save(String key,Object value){
		if(value instanceof Date){
			value = DateTimeUtils.format((Date)value,"yyyy/MM/dd HH:mm:ss");
		}
		this.db.updateRunState(key, StringUtils.valueOf(value));
	}
	
	private void saveGlobal(String key,Object value){
		this.db.updateRunState(key, StringUtils.valueOf(value));
	}
	
	public long getNextSequenceId() {
		long next = nextSequenceId;
		nextSequenceId++;
		this.saveGlobal("nextSequenceId",StringUtils.valueOf(nextSequenceId));
		return next;
	}

	public void updateCurrentFetchState(String current) {
		if((this.currentFetchState !=null && !this.currentFetchState.equals(current)) || (currentFetchState==null && current!=null)){
			this.currentFetchState = current;
			this.save("currentFetchState", currentFetchState);
		}
	}
	
	public void updateCurrentTaskStartTime(Date currentTaskStartTime) {
		this.currentTaskStartTime = currentTaskStartTime;
		this.save("currentTaskStartTime",  currentTaskStartTime);
	}
	
	public void updateCurrentTaskEndTime(Date currentTaskEndTime) {
		this.currentTaskEndTime = currentTaskEndTime;
		this.save("currentTaskEndTime", currentTaskEndTime);
	}
	
	public void updateCurrentTaskStartAndEndTime(Date currentTaskStartTime,Date currentTaskEndTime){
		updateCurrentTaskStartTime(currentTaskStartTime);
		updateCurrentTaskEndTime(currentTaskEndTime);
	}
	
	public void updateLastTaskEndTime(Date lastTaskEndTime){
		this.lastTaskEndTime = lastTaskEndTime;
		this.save("lastTaskEndTime", lastTaskEndTime);
	}
	
	public int getIndexCount() {
		return indexCount.intValue();
	}

	public long getIndexMaxId() {
		return indexMaxId;
	}

	public void setIndexMaxId(long indexMaxId) {
		this.indexMaxId = indexMaxId;
		this.save("indexMaxId", indexMaxId);
	}

	public int getCurrentEntry() {
		return currentEntry;
	}

	public void setCurrentEntry(int currentEntry) {
		this.currentEntry = currentEntry;
		this.save("currentEntry", currentEntry);
	}
	
	public void resetCurrentEntry(){
		this.setCurrentEntry(0);
	}
	
}
