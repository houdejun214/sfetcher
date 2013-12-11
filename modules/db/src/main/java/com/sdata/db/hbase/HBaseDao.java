package com.sdata.db.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.framework.db.hbase.thrift.HBaseClient;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Constants;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.db.BaseDao;
import com.sdata.db.DaoCollection;
import com.sdata.extension.statistic.Statistic;

/**
 * @author zhufb
 *
 */
public class HBaseDao implements BaseDao {
	
	protected DaoCollection storeCollection ;
	protected HBaseClient client;
	protected Statistic statisStore;
	
	public HBaseDao(HBaseClient client,DaoCollection storeCollection){
		this.storeCollection = storeCollection;
		this.client = client;
		this.statisStore = new Statistic(CrawlAppContext.conf);
		
		List<String> cfs = new ArrayList<String>();
		cfs.add(com.framework.db.hbase.Constants.HBASE_DEFAULT_COLUMN_FAMILY);
		for(ColumnFamily scf:storeCollection.getColflys()){
			cfs.add(scf.getName());
		}
		this.client.createTable(storeCollection.getName(),cfs.toArray(new String[cfs.size()]));
	}

	public void save(Map<String,Object> data){
		if(data == null){
			return;
		}
		Object objectId = data.remove(Constants.OBJECT_ID);
		if(objectId == null){
			if(StringUtils.isEmpty(storeCollection.getPrimaryKey())){
				throw new RuntimeException("the property _id of object id is empty and no referce primaryKey!");
			}
			objectId = data.get(storeCollection.getPrimaryKey());
		}
		
		if(objectId == null){
			throw new RuntimeException("the map data row key is empty !");
		}
		
		// remove field deal
		this.removeFields(data);
		
		this.save(objectId,data);
	}
	
	protected void save(Object _id ,Map<String,Object> data){
		// string to bytes for row key
		if(isExists(_id)){
			data.remove(Constants.FETCH_TIME);
		}else{
			// increase
			statisStore.increase(storeCollection.getName());
		}
		// save main data
		Map<String, Map<String, Object>> multiColflyData = getMultiColflyData(data);
		this.client.saveMultiFamily(storeCollection.getName(),_id, multiColflyData);
	}
	
	protected Map<String,Map<String,Object>> getMultiColflyData(Map<String,Object> data){
		Map<String,Map<String,Object>>  result = new HashMap<String, Map<String,Object>>();
	    Iterator<ColumnFamily> iterator = storeCollection.getColflys().iterator();
	    boolean needDCF = true;
		while(iterator.hasNext()){
			ColumnFamily next = iterator.next();
			// * proxy all fields
			if("*".equals(next.getFields())){
				result.put(next.getName(), data);
				needDCF = false;
			}else{
				Map<String, Object> subMap = this.getSubMap(data, next.getFieldList());
				if(subMap.size()>0){
					result.put(next.getName(), subMap);
				}
			}
		}
		if(needDCF){
			result.put(com.framework.db.hbase.Constants.HBASE_DEFAULT_COLUMN_FAMILY, data);
		}
		return result;
	}
	
	protected Map<String,Object> getSubMap(Map<String,Object> data,String[] fields){
		Map<String,Object> result = new HashMap<String,Object>();
		for(String f:fields){
			Object value = data.remove(f);
			if(value != null&&!"".equals(value.toString())){
				result.put(f, value);
			}
		}
		return result;
	}

	protected void removeFields(Map<String,Object> data){
		Iterator<String> iterator = storeCollection.getRemove().iterator();
		while(iterator.hasNext()){
			String field = iterator.next();
			data.remove(field);
		}
	}
	
	/**
	 * select exists
	 * 
	 * @param count
	 * @return
	 */
	public boolean isExists(Object id){
		return client.exists(storeCollection.getName(), id);
	}
	
	/**
	 * delete
	 * 
	 * @param count
	 * @return
	 */
	public void delete(Object id){
		client.delete(storeCollection.getName(), id);
	}
}
