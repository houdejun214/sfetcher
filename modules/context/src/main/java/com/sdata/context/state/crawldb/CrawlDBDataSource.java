package com.sdata.context.state.crawldb;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Configuration;

public class CrawlDBDataSource {
	
	private static MysqlDataSource instance = null;
	private static Object sync = new Object();
	public static MysqlDataSource getDataSource(Configuration conf){
		if(instance == null){
			synchronized(sync){
				if(instance == null){
					String host = conf.get("Host");
					String port = conf.get("Port");
					String db = conf.get("DataBase");
					String userName = conf.get("UserName");
					String password = conf.get("Password");
					instance = new MysqlDataSource(host, port, db, userName, password);
				}
			}
		}
		return instance;
	}
}
