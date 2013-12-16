package com.sdata.live;

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
public class LiveItemDB extends CrawlItemDB {

	protected String[] objectInclude;
	protected String[] objectExclude;
	public LiveItemDB(Configuration conf){
		super(conf);
		String oinclude = conf.get("object.include");
		String oexclude = conf.get("object.exclude");
		if(!StringUtils.isEmpty(oinclude)){
			objectInclude = oinclude.split(",");
		}
		if(!StringUtils.isEmpty(oexclude)){
			objectExclude = oexclude.split(",");
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
	
	@Override
	protected List<Map<String,Object>> queryItemQueue(int topN,String status){
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

	@Override
	public int resetItemStatus(){
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
}
