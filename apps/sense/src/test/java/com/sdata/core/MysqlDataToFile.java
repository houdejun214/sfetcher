package com.sdata.core;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.springframework.jdbc.support.rowset.SqlRowSet;

import net.sf.json.JSONObject;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.nus.next.config.ConfigurationManager;
import com.nus.next.config.NSouce;
import com.nus.next.config.NSouceDB;
import com.nus.next.config.SourceConfig;
import com.sdata.conf.sites.CrawlConfig;
import com.sdata.conf.sites.CrawlConfigManager;
import com.sdata.core.item.CrawlItemDB;

/**
 * @author zhufb
 *
 */
public class MysqlDataToFile {
	
	static String tencent = "hot";
	static String twitter = "twitter";
	static String weibo = "weibo";
	static String tableName = "sc_crawl_item_queue_".concat(weibo);
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		CrawlConfigManager configs = CrawlConfigManager.load("sense");
		CrawlConfig crawlSite = configs.getCurCrawlSite();
		Configuration conf = crawlSite.getConf();
		conf.put("next.crawler.item.queue.table", tableName);
		CrawlItemDB crawlItemDB = new CrawlItemDB(conf);
		String sql = "select parameters from "+tableName;
		SqlRowSet rowset = crawlItemDB.getDataSource().getJdbcTemplate().queryForRowSet(sql, new HashMap<String,Object>());
		StringBuffer sb = new StringBuffer();
		while(rowset.next()){
			append(sb,rowset);
		}
		File file = getFile();
		FileUtils.writeStringToFile(file, sb.toString());
	}
	
	private static void append(StringBuffer sb,SqlRowSet rowset){
		String str = rowset.getString("parameters");
		JSONObject json = JSONObject.fromObject(str);
		Object uid = json.get("uid");
		sb.append(uid).append(":").append(1).append("\r\n");
	}
	
	private static File getFile(){
		String f = "output/".concat(tableName).concat(".txt");
		com.lakeside.core.utils.FileUtils.delete(f);
		com.lakeside.core.utils.FileUtils.insureFileExist(f);
		return new File(f);
	}
}
