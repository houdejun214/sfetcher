package com.sdata.future;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.sdata.context.config.Configuration;
import com.sdata.context.model.QueueStatus;
import com.sdata.core.item.CrawlItemDB;

/**
 * @author zhufb
 *
 */
public class FutureItemDB extends CrawlItemDB {
	
	private String[] tags;
	public FutureItemDB(Configuration conf){
		super(conf);
		String strTags = conf.get("tag", null);
		if(!StringUtils.isEmpty(strTags)){
			this.tags = strTags.split(",");
		}
	}
	
	@Override
	protected List<Map<String,Object>> queryItemQueue(int topN,String status){
		Map<String,Object> parameters = new HashMap<String,Object>();
		StringBuffer sql = new StringBuffer("select * from ");
		sql.append(itemQueueTable);
		sql.append(" where status=:status ");
		this.appendSourceInclude(sql);
		this.appendSourceExclude(sql);
		this.appendTag(sql);
		sql.append(" order by priority_score desc limit :topn ");
		parameters.put("status", status);
		parameters.put("topn", topN);
		List<Map<String, Object>> list = dataSource.getJdbcTemplate().queryForList(sql.toString(), parameters);
		return list;
	}

	protected void appendTag(StringBuffer sql){
		if(tags!=null&&tags.length>0){
			sql.append(" and tags in (");
			for(int i=0;i<tags.length;i++){
				String s = tags[i];
				sql.append("'").append(s).append("'");
				if(i == tags.length -1){
					sql.append(") ");
				}else{
					sql.append(",");
				}
			}
		}
	}
	
	@Override
	public int resetItemStatus() {
		Map<String,Object> parameters = new HashMap<String,Object>();
		StringBuffer sql = new StringBuffer("update ");
		sql.append(itemQueueTable);
		sql.append(" set status=:status where 1 = 1 ");
		this.appendSourceInclude(sql);
		this.appendSourceExclude(sql);
		this.appendTag(sql);
		parameters.put("status", QueueStatus.INIT);
		return dataSource.getJdbcTemplate().update(sql.toString(), parameters);
	}
	
	/**
	 * save item queue data
	 * 
	 * @param map
	 */
	public void saveCrawlItem(Map<String,Object> map){
		String sql = "update  " +itemQueueTable+
				"		 set `priority_score`=:priority_score,`crawler_template_id`=:crawler_template_id" +
				"	   WHERE `crawler_name`=:crawler_name and `source_name`=:source_name and `parameters`=:parameters and `tags`=:tags";
		Map<String, Object> params = new HashMap<String,Object>();
		params.put("crawler_name", map.get("crawlerName"));
		params.put("priority_score", map.get("priorityScore"));
		params.put("status", map.get("status"));
		params.put("crawler_template_id", map.get("crawlerTemplateId"));
		params.put("source_name", map.get("sourceName"));
		params.put("parameters", map.get("parameters"));
		params.put("tags", map.get("tags"));
		int update = dataSource.getJdbcTemplate().update(sql, params);
		if(update==0){
			sql="INSERT INTO "+itemQueueTable+ " (`crawler_name`, `source_name`,`crawler_template_id`, `parameters`,`priority_score`,`status`,`tags`) " +
					" VALUES (:crawler_name, :source_name ,:crawler_template_id, :parameters, :priority_score,  :status,:tags) ";
			dataSource.getJdbcTemplate().update(sql, params);
		}
	}
}