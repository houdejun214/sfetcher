package com.sdata.core.item;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.core.Configuration;
import com.sdata.core.QueueStatus;

/**
 * @author zhufb
 *
 */
public class CrawlItemDB {
	
	private String itemQueueTable = null;
	private MysqlDataSource dataSource;
	private String[] sourceInclude;
	private String[] sourceExclude;
	private String[] objectInclude;
	private String[] objectExclude;
	public CrawlItemDB(Configuration conf){
		this.dataSource = CrawlItemDataSource.getDataSource(conf);
		this.itemQueueTable = conf.get("next.crawler.item.queue.table");
		String sinclude = conf.get("source.include");
		String sexclude = conf.get("source.exclude");
		if(!StringUtils.isEmpty(sinclude)){
			sourceInclude = sinclude.split(",");
		}
		if(!StringUtils.isEmpty(sexclude)){
			sourceExclude = sexclude.split(",");
		}
		String oinclude = conf.get("object.include");
		String oexclude = conf.get("object.exclude");
		if(!StringUtils.isEmpty(oinclude)){
			objectInclude = oinclude.split(",");
		}
		if(!StringUtils.isEmpty(oexclude)){
			objectExclude = oexclude.split(",");
		}
	}

	public List<Map<String,Object>> queryItemQueue(int topN){
		return this.queryItemQueue(topN, QueueStatus.INIT);
	}
	
	public int updateItemComplete(final Long id){
		return this.updateItemStatus(id, QueueStatus.COMPLETE);
	}
	
	public List<Map<String,Object>> queryItemQueue(int topN,String status){

		Map<String,Object> parameters = new HashMap<String,Object>();
		StringBuffer sql = new StringBuffer("select * from ");
		sql.append(itemQueueTable);
		sql.append(" where status=:status and object_status=:object_status");
		this.appendSourceInclude(sql);
		this.appendSourceExclude(sql);
		this.appendObjectInclude(sql);
		this.appendObjectExclude(sql);
		sql.append(" order by priority_score desc limit :topn ");
		parameters.put("status", status);
		parameters.put("object_status", "1");
		parameters.put("topn", topN);
		List<Map<String, Object>> list = dataSource.getJdbcTemplate().queryForList(sql.toString(), parameters);
		return list;
	}

	private void appendSourceInclude(StringBuffer sql){
		if(sourceInclude!=null&&sourceInclude.length>0){
			sql.append(" and source_name in (");
			for(int i=0;i<sourceInclude.length;i++){
				String s = sourceInclude[i];
				sql.append("'").append(s).append("'");
				if(i == sourceInclude.length -1){
					sql.append(") ");
				}else{
					sql.append(",");
				}
			}
		}
	}

	private void appendSourceExclude(StringBuffer sql){
		if(sourceExclude!=null&&sourceExclude.length>0){
			sql.append(" and source_name not in (");
			for(int i=0;i<sourceExclude.length;i++){
				String s = sourceExclude[i];
				sql.append("'").append(s).append("'");
				if(i == sourceExclude.length -1){
					sql.append(") ");
				}else{
					sql.append(",");
				}
			}
		}
	}

	private void appendObjectInclude(StringBuffer sql){
		if(objectInclude!=null&&objectInclude.length>0){
			sql.append(" and object_id in (");
			for(int i=0;i<objectInclude.length;i++){
				String s = objectInclude[i];
				sql.append("'").append(s).append("'");
				if(i == objectInclude.length -1){
					sql.append(") ");
				}else{
					sql.append(",");
				}
			}
		}
	}

	private void appendObjectExclude(StringBuffer sql){
		if(objectExclude!=null&&objectExclude.length>0){
			sql.append(" and object_id not in (");
			for(int i=0;i<objectExclude.length;i++){
				String s = objectExclude[i];
				sql.append("'").append(s).append("'");
				if(i == objectExclude.length -1){
					sql.append(") ");
				}else{
					sql.append(",");
				}
			}
		}
	}
	public int updateItemStatus(final Long id,String status){
		StringBuffer sql = new StringBuffer("update ");
		sql.append(itemQueueTable);
		sql.append(" set status=:status where `id`=:id");
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("status", status);
		parameters.put("id", id);
		return dataSource.getJdbcTemplate().update(sql.toString(), parameters);
	}
	
	public int updateItemStatus(String oldStatus,String newStatus){
		StringBuffer sql = new StringBuffer("update ");
		sql.append(itemQueueTable);
		sql.append(" set status=:newstatus where `status`=:oldstatus");
		Map<String,Object> parameters = new HashMap<String,Object>();
		parameters.put("newstatus", newStatus);
		parameters.put("oldstatus", oldStatus);
		return dataSource.getJdbcTemplate().update(sql.toString(), parameters);
	}
	
	public void saveCrawlItem(Map<String,Object> map){
		String sql = "update  " +itemQueueTable+
				"		 set `priority_score`=:priority_score,`object_status`=:object_status,`entry_url`=:entry_url ,`crawler_template_id`=:crawler_template_id" +
				"	   WHERE `object_id`=:object_id and `crawler_name`=:crawler_name and `source_name`=:source_name and `entry_name`=:entry_name and `fields`=:fields and `parameters`=:parameters";
		Map<String, Object> params = new HashMap<String,Object>();
		params.put("crawler_name", map.get("crawlerName"));
		params.put("object_id", map.get("objectId"));
		params.put("priority_score", map.get("priorityScore"));
		params.put("status", map.get("status"));
		params.put("crawler_template_id", map.get("crawlerTemplateId"));
		params.put("entry_url", map.get("entryUrl"));
		params.put("entry_name", map.get("entryName"));
		params.put("source_name", map.get("sourceName"));
		params.put("fields", map.get("fields"));
		params.put("parameters", map.get("parameters"));
		params.put("object_status", map.get("objectStatus"));
		int update = dataSource.getJdbcTemplate().update(sql, params);
		if(update==0){
			sql="INSERT INTO "+itemQueueTable+ " (`crawler_name`, `source_name`,`entry_name`, `entry_url`,`crawler_template_id`, `parameters`,`fields`, `priority_score`, `object_id`, `status`,`object_status`) " +
					" VALUES (:crawler_name, :source_name ,:entry_name, :entry_url,:crawler_template_id, :parameters,:fields, :priority_score, :object_id, :status, :object_status) ";
			dataSource.getJdbcTemplate().update(sql, params);
		}
	}
	
	public int resetItemStatus() {
		Map<String,Object> parameters = new HashMap<String,Object>();
		StringBuffer sql = new StringBuffer("update ");
		sql.append(itemQueueTable);
		sql.append(" set status=:status where 1 = 1 ");
		this.appendSourceInclude(sql);
		this.appendSourceExclude(sql);
		this.appendObjectInclude(sql);
		this.appendObjectExclude(sql);
		parameters.put("status", QueueStatus.INIT);
		return dataSource.getJdbcTemplate().update(sql.toString(), parameters);
	}
	public MysqlDataSource getDataSource() {
		return dataSource;
	}
}
