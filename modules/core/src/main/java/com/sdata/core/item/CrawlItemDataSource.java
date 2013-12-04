package com.sdata.core.item;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Configuration;

/**
 * @author zhufb
 *
 */
public class CrawlItemDataSource  {

	private static MysqlDataSource source;
	private static Object syn = new Object();
	public static MysqlDataSource getDataSource(Configuration conf){
		if(source == null){
			synchronized (syn) {
				if(source == null){
					String host = conf.get("next.crawler.item.host");
					String port = conf.get("next.crawler.item.port");
					String db = conf.get("next.crawler.item.dataBase");
					String userName = conf.get("next.crawler.item.userName");
					String password = conf.get("next.crawler.item.password");
					source = new MysqlDataSource(host, port, db, userName, password);
				}
			}
		}
		return source;
	}
}
