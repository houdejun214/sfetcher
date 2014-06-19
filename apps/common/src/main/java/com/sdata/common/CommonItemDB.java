package com.sdata.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.model.QueueStatus;
import com.sdata.core.item.CrawlItemDB;

/**
 *
 */
public class CommonItemDB extends CrawlItemDB {

	public CommonItemDB(Configuration conf){
		super(conf);
	}
	private static CommonItemDB db;
	public static CommonItemDB getInstance(){
		if (db == null) {
			synchronized (CommonItemDB.class) {
				if (db == null) {
					db = new CommonItemDB(CrawlAppContext.conf);
				}
			}
		}
		return db;
	}

	@Override
	protected List<Map<String,Object>> queryItemQueue(int topN,String status){
		Map<String,Object> parameters = new HashMap<String,Object>();
		StringBuffer sql = new StringBuffer("select * from ");
		sql.append(itemQueueTable);
		sql.append(" where status=:status ");
		this.appendSourceInclude(sql);
		this.appendSourceExclude(sql);
		sql.append(" order by priority_score desc limit :topn ");
		parameters.put("status", status);
		parameters.put("topn", topN);
		List<Map<String, Object>> list = dataSource.getJdbcTemplate().queryForList(sql.toString(), parameters);
		return list;
	}

	@Override
	public int resetItemStatus(){
		Map<String,Object> parameters = new HashMap<String,Object>();
		StringBuffer sql = new StringBuffer("update ");
		sql.append(itemQueueTable);
		sql.append(" set status=:status where 1 = 1 ");
		this.appendSourceInclude(sql);
		this.appendSourceExclude(sql);
		parameters.put("status", QueueStatus.INIT);
		return dataSource.getJdbcTemplate().update(sql.toString(), parameters);
	}
}
