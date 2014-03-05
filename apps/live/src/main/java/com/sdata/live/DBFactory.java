package com.sdata.live;

import com.sdata.context.config.CrawlAppContext;
import com.sdata.core.item.CrawlItemDB;
import com.sdata.live.ds.DSItemDB;
import com.sdata.live.history.HistoryItemDB;

/**
 * @author zhufb
 * 
 */
public class DBFactory {

	private static Object syn = new Object();
	private static CrawlItemDB db = null;

	public static CrawlItemDB getItemDB() {
		if (db == null) {
			synchronized (syn) {
				if (db == null) {
					String crawl = CrawlAppContext.conf.get("crawlName", CrawlerEnum.Live.getName());
					if (CrawlerEnum.Live.getName().equals(crawl)) {
						db = new LiveItemDB(CrawlAppContext.conf);
					}else if (CrawlerEnum.History.getName().equals(crawl)) {
						db = new HistoryItemDB(CrawlAppContext.conf);
					}else if (CrawlerEnum.Ds.getName().equals(crawl)) {
						db = new DSItemDB(CrawlAppContext.conf);
					}else {
						throw new RuntimeException("crawler "+ crawl +" is not support,please start another crawler!");
					}
				}
			}
		}
		return db;
	}
}
