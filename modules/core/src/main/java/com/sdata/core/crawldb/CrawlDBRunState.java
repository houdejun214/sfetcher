package com.sdata.core.crawldb;

import java.util.Map;

public interface CrawlDBRunState {

	/**
	 * query the runstate map of a site
	 * @param siteName
	 * 
	 * @return
	 */
	public  Map<String, String> queryAllRunState();

	public  String getRunState(final String key);

	/**
	 * this method may be performed  from different threads
	 * @param key
	 * @param val
	 * @return
	 */
	public Boolean updateRunState(final String key,	final String val);
	
	public Boolean lock();
	
	public Boolean unlock();

}