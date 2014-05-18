package com.sdata.context.state.crawldb.impl;

import java.util.Map;

import org.apache.commons.lang.WordUtils;

import com.lakeside.data.redis.RedisDB;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.CrawlDBRunState;
import com.sdata.context.state.crawldb.CrawlDBRedis;

/**
 * 所有的runstate存储在Sdatacrawler:RunState命名空间下，
 * 
 * 每个Crawler分别对应主命名空间中一个hash结构的key-value, key是crawlerName,value是hash结构
 * 
 * @author houdejun
 *
 */
public class CrawlDBRunStateImpl implements CrawlDBRunState {
	
	private static final String RunstateRoot="RunState:";
	private static final String CrawlLock="lock";
	
	private RedisDB db;

	private String crawlName;
	
	public CrawlDBRunStateImpl(Configuration conf){
		crawlName = conf.get("crawlName");
		if(conf.containsKey("CrawlDbTableName")){
			crawlName = conf.get("CrawlDbTableName");
		}
		String namespace = RunstateRoot + WordUtils.capitalize(crawlName);
		db = CrawlDBRedis.getRedisDB(conf,namespace);
	}

	public Map<String,String> queryAllRunState(){
		return db.hgetAll(crawlName);
	}
	
	public String getRunState(final String key){
		return db.hget(crawlName, key);
	}
	
	public Boolean updateRunState(final String key,final String val){
		return db.hset(crawlName, key, val);
	}
}