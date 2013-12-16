package com.sdata.live.resource;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Configuration;
import com.sdata.core.item.CrawlDataSource;

/**
 * @author zhufb
 * 
 */
public class ResourceDB {
	
	protected MysqlDataSource dataSource;
	protected String resourceTable;

	public ResourceDB(Configuration conf) {
		this.dataSource = CrawlDataSource.getDataSource(conf);
		this.resourceTable = conf.get("crawl.resource.table");
	}

	public Map<String,Object> getResource(String source) {
		return getResource(source,null);
	}

	public Map<String,Object> getResource(String source,String type) {
		Map<String, Object> parameters = new HashMap<String, Object>();
		StringBuffer sql = new StringBuffer("select * from  ");
		sql.append(resourceTable);
		sql.append(" where 1 = 1 ");
		sql.append(" and source =:source ");
		sql.append(StringUtils.isEmpty(type)?"":" and type = :type");
		sql.append(" and ifnull(status,0) =:status ");
		sql.append(" and ifnull(available,1) = 1  ");
		sql.append(" order by last_return_time,last_use_time ");
		sql.append(" limit 1 ");
		parameters.put("source", source);
		parameters.put("status", ResourceStatus.INIT);
		parameters.put("type", type);
		List<Map<String, Object>> list = dataSource.getJdbcTemplate().queryForList(sql.toString(), parameters);
		if(list == null||list.size() == 0){
			return null;
		}
		return list.get(0);
	}
	
	public void updateUsing(Long id) {
		Map<String, Object> parameters = new HashMap<String, Object>();
		StringBuffer sql = new StringBuffer("update  ");
		sql.append(resourceTable);
		sql.append(" set status = :status ");
		sql.append(", last_use_time = :time ");
		sql.append(" where 1 = 1 ");
		sql.append(" and id = :id ");
		parameters.put("status", ResourceStatus.USING);
		parameters.put("time", new Date());
		parameters.put("id", id);
		dataSource.getJdbcTemplate().update(sql.toString(), parameters);
	}
	

	public void returnResource(Long id) {
		Map<String, Object> parameters = new HashMap<String, Object>();
		StringBuffer sql = new StringBuffer("update  ");
		sql.append(resourceTable);
		sql.append(" set status = :status ");
		sql.append(", last_return_time = :time ");
		sql.append(" where 1 = 1 ");
		sql.append(" and id = :id ");
		parameters.put("status", ResourceStatus.INIT);
		parameters.put("time", new Date());
		parameters.put("id", id);
		dataSource.getJdbcTemplate().update(sql.toString(), parameters);
	}
	
}
