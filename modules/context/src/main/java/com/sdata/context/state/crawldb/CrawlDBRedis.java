package com.sdata.context.state.crawldb;

import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.lakeside.data.redis.RedisDB;
import com.sdata.context.config.Configuration;

/**
 * @author zhufb
 *
 */
public class CrawlDBRedis  {
	
	private static String ROOT = "Sdatacrawler:";
	private static Object sync = new Object();
	private static Map<String,RedisDB> dbs = new HashMap<String,RedisDB>();
	
	public static RedisDB getRedisDB(Configuration conf,String namespace){
		if(!dbs.containsKey(namespace)){
			synchronized(sync){
				if(!dbs.containsKey(namespace)){
					String namespacePrefix = ROOT + namespace;
					String host = conf.get("RedisHost");
					int port = conf.getInt("RedisPort",6379);
					String password = conf.get("RedisPassword");
                    Boolean auth = conf.getBoolean("RedisAuth", false);
                    if(auth){
                        dbs.put(namespace,new RedisDB(host,port,password,namespacePrefix));
                    }else{
                        dbs.put(namespace,new RedisDB(host,port,namespacePrefix));
                    }

				}
			}
		}
		return dbs.get(namespace);
	}
}
