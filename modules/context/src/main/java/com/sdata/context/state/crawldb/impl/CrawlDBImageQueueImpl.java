package com.sdata.context.state.crawldb.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.lang.StringUtils;

import com.lakeside.data.redis.RedisDB;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.crawldb.CrawlDBImageQueue;
import com.sdata.context.state.crawldb.CrawlDBImageQueueEnum;

/**
 * @author zhufb
 *
 */
public class CrawlDBImageQueueImpl implements CrawlDBImageQueue {
	private static final String QueueNamePrefix="Sdatacrawler:ImageQueue:";
	private static final String QueueName="List";
	private static final String SetName="Set";
	private RedisDB db;
	
	public CrawlDBImageQueueImpl(Configuration conf){
		String namespacePrefix= QueueNamePrefix.concat(conf.get("image.namespace"));
		String host = conf.get("RedisHost");
		int port = conf.getInt("RedisPort",6379);
		String password = conf.get("RedisPassword");
		db = new RedisDB(host,port,password,namespacePrefix);
	}

	public Boolean isImageExists(String url, String source) {
		return db.sismember(SetName,this.encodeToJson(source,url));
	}

	public void insertImageQueue(List<String> list, String source) {
		for(int i=0;i<list.size();i++){
			 String json = this.encodeToJson(source, list.get(i));
			 if(!db.sismember(SetName,json)){
				 db.rpush(QueueName, json);
				 db.sadd(SetName, json);
			 }
		}
	}

	public void updateImageQueue(String id, String status) {
		return;
	}

	public List<Map<String,Object>> queryImageQueue(int count) {
		List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
		for(int i=0;i<count;i++){
			list.add(this.getOneImage());
		}
		return list;
	}
	
	private String encodeToJson(String s,String v){
		JSONObject json  = new JSONObject();
		json.put(CrawlDBImageQueueEnum.SOURCE.value(), s);
		json.put(CrawlDBImageQueueEnum.URL.value(), v);
		return json.toString();
	}

	private Map<String, Object> decodeToMap(String s){
		return JSONObject.fromObject(s);
	}

	public Map<String, Object> getOneImage() {
		String str = db.lpop(QueueName);
		if(StringUtils.isEmpty(str)){
			return null;
		}
		db.sremove(SetName, str);
		return this.decodeToMap(str);
	}
}
