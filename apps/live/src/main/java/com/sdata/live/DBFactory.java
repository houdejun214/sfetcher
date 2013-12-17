package com.sdata.live;

import com.sdata.context.config.CrawlAppContext;
import com.sdata.core.resource.ResourceDB;
import com.sdata.live.history.HistoryItemDB;

/**
 * @author zhufb
 * 
 */
public class DBFactory {

	private static Object syn = new Object();
	private static LiveItemDB db = null;
	private static ResourceDB rdb = null;

	public static LiveItemDB getItemDB() {
		if (db == null) {
			synchronized (syn) {
				if (db == null) {
					String crawl = CrawlAppContext.conf.get("crawlName", "live");
					if ("live".equals(crawl)) {
						db = new LiveItemDB(CrawlAppContext.conf);
					} else {
						db = new HistoryItemDB(CrawlAppContext.conf);
					}
				}
			}
		}
		return db;
	}
	
	public static ResourceDB getResourceDB() {
		if (rdb == null) {
			synchronized (syn) {
				if (rdb == null) {
					rdb = new ResourceDB(CrawlAppContext.conf);
				}
			}
		}
		return rdb;
	}
}
