package com.sdata.live.history;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.live.LiveItemDB;

/**
 * @author zhufb
 *
 */
public class HistoryItemDB extends LiveItemDB {
	
	public HistoryItemDB(Configuration conf){
		super(conf);
	}
	
	@Override
	protected List<Map<String,Object>> queryItemQueue(int topN,String status){
		Map<String,Object> parameters = new HashMap<String,Object>();
		StringBuffer sql = new StringBuffer("select * from ");
		sql.append(itemQueueTable);
		sql.append(" where status=:status and object_status=:object_status");
		sql.append(" and UNIX_TIMESTAMP（create_time）>= UNIX_TIMESTAMP() - IFNULL(period,0) ");
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
	
}
