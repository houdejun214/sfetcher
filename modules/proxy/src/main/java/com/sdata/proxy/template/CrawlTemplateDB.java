package com.sdata.proxy.template;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Configuration;
import com.sdata.core.item.CrawlDataSource;

/**
 * @author zhufb
 *
 */
public class CrawlTemplateDB {
	
	private String templateTable = null;
	private MysqlDataSource dataSource;
	public CrawlTemplateDB(Configuration conf){
		this.dataSource = CrawlDataSource.getDataSource(conf);
		this.templateTable = conf.get("next.sense.template.table");
	}
	
	public List<Map<String,Object>> query(){
		StringBuffer sql = new StringBuffer("select * from ");
		sql.append(templateTable);
		List<Map<String, Object>> list = dataSource.getJdbcTemplate().queryForList(sql.toString(), new HashMap<String,Object>());
		return list;
	}
}
