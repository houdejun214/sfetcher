package com.sdata.image;

import java.util.Map;

import com.sdata.core.Configuration;
import com.sdata.core.crawldb.CrawlDB;
import com.sdata.core.crawldb.impl.CrawlDBImpl;

/**
 * @author zhufb
 * 
 *         typical producer-consumer scenario
 * 
 */
public class ImageQueue {
	private CrawlDB db;
	public ImageQueue(Configuration config){
		db = new CrawlDBImpl(config);
	}
	public Map<String, Object> get() {
		return db.getOneImage();
	}
}
