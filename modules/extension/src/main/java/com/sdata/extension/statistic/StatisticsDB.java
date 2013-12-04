package com.sdata.extension.statistic;

import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

import com.sdata.context.config.Configuration;

/**
 * @author zhufb
 *
 */
public class StatisticsDB {

	private static final Logger log = LoggerFactory.getLogger("StatisticsDB");
	private static String dbName;
	private JedisPool pool;
	
	public StatisticsDB(Configuration config){
		String host = config.get("RedisHost");
		int port = config.getInt("RedisPort",6379);
		String password = config.get("RedisPassword");
		dbName = config.get("StatisticsRedisDBName");
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		pool = new JedisPool(poolConfig,host,port,Protocol.DEFAULT_TIMEOUT,password);
	}

	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#querySiteRunStateMap(java.lang.String)
	 */
	public Map<String,String> query(){
		Jedis jedis=null;
		Map<String, String> maps;
		try {
			jedis = pool.getResource();
			maps = jedis.hgetAll(dbName);
		} finally {
			if(jedis!=null){
				pool.returnResource(jedis);
			}
		}
		return maps;
	}
	
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#getRunState(java.lang.String, java.lang.String)
	 */
	public String get(final String key){
		Jedis jedis=null;
		String value;
		try {
			jedis = pool.getResource();
			value = jedis.hget(dbName, key);
		} finally {
			if(jedis!=null){
				pool.returnResource(jedis);
			} 
		}
		return value;
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#updateRunState(java.lang.String, java.lang.String, java.lang.String)
	 */
	public Boolean update(final String key,final String val){
		Jedis jedis =null;
		try {
			jedis = pool.getResource();
			jedis.hset(dbName, key, val);
		} finally {
			if(jedis!=null){
				pool.returnResource(jedis);
			}
		}
		return true;
	}
	

	public long del(String key){
		Jedis jedis =null;
		try {
			jedis = pool.getResource();
			Long ret = jedis.hdel(dbName, key);
			return ret;
		} finally {
			if(jedis!=null){
				pool.returnResource(jedis);
			}
		}
	}

	public void increase(String key){
		Jedis jedis =null;
		try {
			jedis = pool.getResource();
			jedis.hincrBy(dbName, key, 1);
		} finally {
			if(jedis!=null){
				pool.returnResource(jedis);
			}
		}
	}

	public void increase(String sub,String key){
		Jedis jedis =null;
		try {
			jedis = pool.getResource();
			jedis.hincrBy(dbName.concat(":").concat(sub), key, 1);
		} finally {
			if(jedis!=null){
				pool.returnResource(jedis);
			}
		}
	}
}
