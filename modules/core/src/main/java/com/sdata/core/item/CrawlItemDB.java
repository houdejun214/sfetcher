package com.sdata.core.item;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.lakeside.data.sqldb.MysqlDataSource;
import com.sdata.context.config.Configuration;
import com.sdata.context.model.QueueStatus;

/**
 * @author zhufb
 *
 */
public abstract class CrawlItemDB {
	
	protected String itemQueueTable = null;
	protected MysqlDataSource dataSource;
	protected String[] sourceInclude;
	protected String[] sourceExclude;
	protected String[] objectInclude;
	protected String[] objectExclude;
	
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

	/**
	 * @param topN
	 * @param status
	 * @return
	 */
	protected abstract List<Map<String,Object>> queryItemQueue(int topN,String status);
	
	public List<Map<String,Object>> queryItemQueue(int topN){
		return this.queryItemQueue(topN, QueueStatus.INIT);
	}
	
	public int updateItemComplete(final Long id){
		return this.updateItemStatus(id, QueueStatus.COMPLETE);
	}
	

	protected void appendSourceInclude(StringBuffer sql){
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

	protected void appendSourceExclude(StringBuffer sql){
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

	protected void appendObjectInclude(StringBuffer sql){
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

	protected void appendObjectExclude(StringBuffer sql){
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
