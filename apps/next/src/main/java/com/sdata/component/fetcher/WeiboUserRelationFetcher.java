package com.sdata.component.fetcher;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sdata.component.parser.WeiboUserRelationParser;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.CrawlAppContext;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.crawldb.CrawlDB;
import com.sdata.core.fetcher.SdataFetcher;
import com.sdata.core.util.WebPageDownloader;

/**
 * Weibo UserRelation info fetcher implement
 * 
 * @author zhufb
 *
 */
public class WeiboUserRelationFetcher extends SdataFetcher{
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.WeiboStarFetcher");
	
	private static final String weiboStarUrl="http://data.weibo.com/top/influence/famous?class=0001&type=day";
	
	private CrawlDB crawlDB;
	protected Queue<Map<String, Object>> queue;
	int crawlDepth; 
	boolean hasQueue = false;
	boolean complete = false;
	
	public WeiboUserRelationFetcher(Configuration conf,RunState state) {
		this.setConf(conf);
		this.setRunState(state);
	    this.parser =  new WeiboUserRelationParser(conf,state);
		crawlDB = CrawlAppContext.db;
		crawlDepth = this.getConfInt(Constants.QUEUE_DEPTH_MAX, 3);
		this.initQueueList();
		
	}

	/**
	 * init stars（first 20 - 100） info 
	 * 
	 * @param weiboTopicParser
	 * @return
	 */
	private void initQueueList(){
		synchronized (this) {
			 if(queue != null&&queue.size() > 0) return;
			 List<Map<String, Object>> list = this.queryQueueUsers();
			 if(!hasQueue&&(list==null||list.size() == 0)) {
				 String content = WebPageDownloader.download(weiboStarUrl);
				 list = ((WeiboUserRelationParser)parser).parseUserRelationList(content);
				 this.appendQueueUsers(list);
			 }
			 queue = new ArrayBlockingQueue<Map<String,Object>>(Constants.FETCH_COUNT*2);
			 queue.addAll(list);
			 hasQueue = true;
		}
	}
	
	/* 
	 * fetch  list info 
	 *
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatumList()
	 */
	@Override
	public List<FetchDatum> fetchDatumList() {
		// get next item
		Map<String, Object> current = getNextItem();
		//if current is null it's mean it's over
		if(current == null) return null;
		int depth = Integer.parseInt(String.valueOf(current.get(Constants.QUEUE_DEPTH)));
		String uid = String.valueOf(current.get(Constants.QUEUE_KEY));
		// get user's friends save items to sqlite 
		List<Map<String, Object>> list = ((WeiboUserRelationParser)parser).parseUserRelationList(uid,depth+1);
		if(depth < crawlDepth)	this.appendQueueUsers(list);
		// current item finish 
		this.finishItem(current);
		return parseListToDatum(current,uid,list);
	}

	/* 
	 * fetch user's info
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.component.fetcher.SdataFetcher#fetchDatum(com.sdata.component.FetchDatum)
	 */
	@Override
	public FetchDatum fetchDatum(FetchDatum datum) {
		return datum;
	}

	/**
	 * Parse json data to datum 
	 * @param JSONObject json
	 * @return FetchDatum
	 */
	@SuppressWarnings("unchecked")
	public List<FetchDatum> parseListToDatum(Map<String, Object> current,String uid,List<Map<String, Object>> list) {
		List<FetchDatum> result = new ArrayList<FetchDatum>();
		FetchDatum datum = new FetchDatum();
		Map<String,Object> map = new HashMap<String,Object>();
		Iterator<Map<String, Object>> iterator = list.iterator();
		List<Map<String, Object>> listeners =new ArrayList<Map<String, Object>>();
		List<Map<String, Object>> followers =new ArrayList<Map<String, Object>>();
		while(iterator.hasNext()){
			Map<String, Object> next = iterator.next();
			if(next.containsKey(Constants.USER_RELATION_FRIENDS))
				listeners.add((Map<String, Object>)next.get(Constants.USER_RELATION_FRIENDS));
			else
				followers.add((Map<String, Object>)next.get(Constants.USER_RELATION_FOLLOWS));
		}
		map.put(Constants.OBJECT_ID, uid);
		map.put(Constants.USER_ID, uid);
		map.put(Constants.USER_NAME, current.get(Constants.QUEUE_NAME));
		map.put(Constants.USER_HOMEPAGE, current.get(Constants.QUEUE_URL));
		map.put(Constants.USER_LISTENS, listeners);
		map.put(Constants.USER_FOLLOWS, followers);
		if(current.containsKey(Constants.USER)) map.put(Constants.USER, current.get(Constants.USER));
		datum.setId(uid);
		datum.setMetadata(map);
		result.add(datum);
		return result;
	}
	
	private List<Map<String, Object>> appendQueueUsers(List<Map<String, Object>> list) {
		this.crawlDB.insertQueueObjects(list);
		return list;
	}
	
	private void updateQueueUser(String uid) {
		this.crawlDB.updateQueueComplete(uid);
	}
	
	protected List<Map<String, Object>> queryQueueUsers() {
		return this.crawlDB.queryQueue(Constants.FETCH_COUNT);
	}
	
	/**
	 * move to next crawl instance
	 */
	protected Map<String,Object> getNextItem(){
		synchronized(this){
			if(queue==null||queue.size() == 0){
				initQueueList();
			}
			if(queue==null||queue.size() == 0){
				complete = true;
				return null;
			}
			return queue.poll(); 
		}
	}
	
	/**
	 * @param current
	 */
	protected  void finishItem(Map<String,Object> current){
		this.updateQueueUser(String.valueOf(current.get(Constants.QUEUE_KEY)));
	}

	@Override
	public boolean isComplete(){
		return false;
	}

	@Override
	protected void moveNext() {
		
	}
}
