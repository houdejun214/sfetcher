package com.sdata.context.config;

import com.sdata.context.state.RunState;
import com.sdata.context.state.crawldb.CrawlDB;

public class CrawlAppContext {
	public static CrawlDB db;
	public static RunState state;
	public static Configuration conf;
	public static int startServerPort=8080;
}
