package com.sdata.core;

import com.sdata.core.crawldb.CrawlDB;


public class CrawlAppContext {
	public static CrawlDB db;
	public static RunState state;
	public static Configuration conf;
	public static int startServerPort=8080;
}
