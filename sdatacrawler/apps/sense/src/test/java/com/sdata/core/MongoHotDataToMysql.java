package com.sdata.core;

import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import com.lakeside.data.mongo.MongoDBConnectionManager;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.sdata.conf.sites.CrawlConfig;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.core.item.CrawlItemDB;

/**
 * @author zhufb
 *
 */
public class MongoHotDataToMysql {
	
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CrawlConfigManager configs = CrawlConfigManager.load("sense");
		CrawlConfig crawlSite = configs.getCurCrawlSite();
		
		Configuration conf = crawlSite.getConf();
		conf.put("next.crawler.item.queue.table", "sc_crawl_item_queue_hot");
		CrawlItemDB crawlItemDB = new CrawlItemDB(conf);
		DB db = MongoDBConnectionManager.getConnection("172.18.109.20", "tencentOthers", null, null);
		DBCollection collection = db.getCollection("famous");
		DBCursor cursor = collection.find();

		JSONObject json = new JSONObject();
		json.put("timeRange", "2013-07-20 00:00:00/*");
		Map<String,Object> map = new HashMap<String, Object>();
		map.put("crawlerName", "tencentHot");
		map.put("objectId", 1);
		map.put("priorityScore", 100);
		map.put("status", 0);
		map.put("entryUrl", "http://t.qq.com");
		map.put("entryName", "tencent entry");
		map.put("sourceName", "tencent");
		map.put("fields", "{}");
		map.put("parameters", null);
		map.put("objectStatus", "1");
		while(cursor.hasNext()){
			DBObject next = cursor.next();
			String uname = next.get("name").toString();
			json.put("uid", uname);
			map.put("parameters", json.toString());
			crawlItemDB.saveCrawlItem(map);
		}
	}
}
