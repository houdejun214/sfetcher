package com.sdata.future;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.core.item.CrawlItemDB;

/**
 * @author zhufb
 *
 */
public class FutureItemDB extends CrawlItemDB {
	
	public FutureItemDB(Configuration conf){
		super(conf);
	}
	
	protected List<Map<String,Object>> queryItemQueue(int topN,String status){
		Map<String,Object> parameters = new HashMap<String,Object>();
		StringBuffer sql = new StringBuffer("select * from ");
		sql.append(itemQueueTable);
		sql.append(" where status=:status ");
		sql.append(" order by priority_score desc limit :topn ");
		parameters.put("status", status);
		parameters.put("topn", topN);
		List<Map<String, Object>> list = dataSource.getJdbcTemplate().queryForList(sql.toString(), parameters);
		return list;
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
