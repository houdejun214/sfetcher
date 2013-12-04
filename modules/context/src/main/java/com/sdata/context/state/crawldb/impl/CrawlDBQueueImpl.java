package com.sdata.context.state.crawldb.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.WordUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.DefaultTransactionDefinition;

import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.model.QueueStatus;
import com.sdata.context.state.crawldb.CrawlDBQueue;
import com.sdata.context.state.crawldb.CrawlDBSqlSupport;

public class CrawlDBQueueImpl extends CrawlDBSqlSupport implements CrawlDBQueue {
	
	private static final Logger log = LoggerFactory.getLogger("SdataCrawler.CrawlDBQueueImpl");
	
	public String CrawlQueueTableName="CrawlQueue";
	public CrawlDBQueueImpl(Configuration conf){
		super(conf);
		String crawlName = conf.get("CrawlName");
		if(conf.containsKey("CrawlDbTableName")){
			crawlName = conf.get("CrawlDbTableName");
		}
		String name = WordUtils.capitalize(crawlName,new char[]{' ','-','_'});
		name =name.replaceAll("[-_]", "");
		CrawlQueueTableName="CrawlQueue"+name;
		
		this.initTables(CrawlQueueTableName,"CrawlDBQueueScriptFile");
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#queryQueue(int)
	 */
	public List<Map<String,Object>> queryQueue(int topN){
		return queryQueue(topN,false);
	}
	
	/* 
	 * @see com.sdata.core.CrawlDB#queryQueue(int, java.lang.Boolean)
	 */
	public List<Map<String,Object>> queryQueue(String tableName,int topN){
		List<Map<String, Object>> list = queryQueue(tableName,topN,false);
		return list;
	}
	
	public List<Map<String,Object>> queryQueue(String tableName,int topN,int lastId){
		String sql="select * from "+tableName+" where id>=:lastid order by id asc limit :topn";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("topn", topN);
		parameters.put("lastid", lastId);
		List<Map<String, Object>> list = this.jdbcTemplate.queryForList(sql, parameters);
		return list;
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#queryQueue(int, java.lang.String)
	 */
	public List<Map<String,Object>> queryQueue(int topN,String dispose){
		return queryQueue(topN,Boolean.valueOf(dispose));
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#queryQueueTotoalCount()
	 */
	public int queryQueueTotoalCount(){
		String sql="Select count(*) from "+CrawlQueueTableName;
		Map<String, ?> paramMap=null;
		int result = this.jdbcTemplate.queryForInt(sql, paramMap);
		return result;
	}
	
	/* 
	 * @see com.sdata.core.CrawlDB#queryQueue(int, java.lang.Boolean)
	 */
	public List<Map<String,Object>> queryQueue(int topN,Boolean dispose){
		List<Map<String, Object>> list = this.queryQueue(CrawlQueueTableName, topN, dispose);
		return list;
	}
	
	public List<Map<String,Object>> queryQueue(String tableName, int topN,Boolean dispose){
		String sql="select * from "+tableName+" where status=:status order by id asc limit :topn";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("status", dispose?QueueStatus.COMPLETE:QueueStatus.INIT);
		parameters.put("topn", topN);
		List<Map<String, Object>> list = this.jdbcTemplate.queryForList(sql, parameters);
		return list;
	}
	
	public List<Map<String,Object>> queryQueueByStatus(int topN,String status){
		String sql="select * from "+CrawlQueueTableName+" where status=:status order by id asc limit :topn";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("status", status);
		parameters.put("topn", topN);
		List<Map<String, Object>> list = this.jdbcTemplate.queryForList(sql, parameters);
		return list;
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#updateQueueDispose(java.lang.String)
	 */
	public Boolean updateQueueComplete(final String key){
		this.updateQueueStatus(key, QueueStatus.COMPLETE);
		return true;
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#updateQueueDispose(java.lang.String)
	 */
	public Boolean updateQueueStatus(final String key,String status){
		String sql="update "+CrawlQueueTableName+" set status=:status where `key`=:key";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("status", status);
		parameters.put("key", key);
		this.jdbcTemplate.update(sql, parameters);
		return true;
	}
	
	public Boolean changeQueueStatus(String oldStatus,String newStatus){
		String sql="update "+CrawlQueueTableName+" set status=:newstatus where `status`=:oldstatus";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("newstatus", newStatus);
		parameters.put("oldstatus", oldStatus);
		int x = this.jdbcTemplate.update(sql, parameters);
		return true;
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#insertQueueObjects(java.util.List)
	 */
	public Boolean insertQueueObjects(final List<Map<String,Object>> objects){
		DataSourceTransactionManager transactionManager = this.dataSource.getTransactionManager();
		DefaultTransactionDefinition def = this.dataSource.getTransactionDefinition();
		TransactionStatus transaction = transactionManager.getTransaction(def);
		try{
			for (Map<String, Object> obj : objects) {
				String key = obj.get(Constants.QUEUE_KEY).toString();
				String url = StringUtils.valueOf(obj.get(Constants.QUEUE_URL));
				int depth = Integer.valueOf(StringUtils.valueOf(obj.get(Constants.QUEUE_DEPTH)));
				Object status = obj.get(Constants.QUEUE_STATUS);
				if(status == null){
					status = QueueStatus.INIT;
				}
				Map<String,Object> parameters = new HashMap<String,Object>();
				parameters.put("key", key);
				String sql="select count(1) from "+CrawlQueueTableName+" where `key`=:key";
				int count = jdbcTemplate.queryForInt(sql, parameters);
				if(count <= 0){
					sql="insert into "+CrawlQueueTableName+" (`key`,url, depth,status) values (:key,:url,:depth,:status)";
					parameters.put("url", url);
					parameters.put("depth", depth);
					parameters.put("status", status);
					try {
						jdbcTemplate.update(sql, parameters);
					} catch (DuplicateKeyException e) {
						log.warn("Duplicate key [{}]",key);
					}
				}
			}
			transactionManager.commit(transaction);
			transaction=null;
		}finally{
			if(transaction!=null){
				transactionManager.rollback(transaction);
			}
		}
		return false;
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#deleteQueueByKey(java.lang.String)
	 */
	public Boolean deleteQueueByKey(String key){
		String sql="delete from "+CrawlQueueTableName+" where `key`=:key";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("key", key);
		this.jdbcTemplate.update(sql, parameters);
		return true;
	}

	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#deleteQueueByKey(java.lang.String)
	 */
	public void deleteQueue(){
		String sql="delete from "+CrawlQueueTableName;
		Map<String,Object> parameters = new HashMap<String,Object>();
		this.jdbcTemplate.update(sql,parameters);
	}
	
	/* (non-Javadoc)
	 * @see com.sdata.core.CrawlDB#isQueueDepthComplete(java.lang.String)
	 */
	public Boolean isQueueDepthComplete(final String depth){
		String sql="Select count(*) from "+CrawlQueueTableName+" where depth=:depth";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("depth", depth);
		int result = this.jdbcTemplate.queryForInt(sql, parameters);
		if(result>0){
			sql="Select count(*) from "+CrawlQueueTableName+" where depth=:depth and status!=:status";
			parameters = new HashMap<String,Object>();
			parameters.put("depth", depth);
			parameters.put("status", QueueStatus.COMPLETE);
			result = this.jdbcTemplate.queryForInt(sql, parameters);
			return result==0;
		}else{
			//当前depth下没有数据时，认为此depth没有完成
			return false;
		}
	}

	public void resetQueueStatus() {
		String sql="update "+CrawlQueueTableName+" set status=:status ";
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("status", QueueStatus.INIT);
		this.jdbcTemplate.update(sql, parameters);
	}
}
