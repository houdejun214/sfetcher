package com.sdata.db.hbase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.framework.db.hbase.thrift.HBaseClient;
import com.lakeside.core.utils.StringUtils;
import com.sdata.context.config.Constants;
import com.sdata.db.BaseDao;
import com.sdata.db.ColumnFamily;
import com.sdata.db.DaoCollection;

/**
 * @author zhufb
 *
 */
public class HBaseDao implements BaseDao {
	
	protected DaoCollection daoCollection ;
	protected HBaseClient client;
	protected String collectionName;
	
	public HBaseDao(HBaseClient client,DaoCollection daoCollection){
		this.daoCollection = daoCollection;
		this.client = client;
		this.collectionName = daoCollection.getName();
		List<String> cfs = new ArrayList<String>();
		cfs.add(com.framework.db.hbase.Constants.HBASE_DEFAULT_COLUMN_FAMILY);
		for(ColumnFamily scf:daoCollection.getColflys()){
			cfs.add(scf.getName());
		}
		this.client.createTable(daoCollection.getName(),cfs.toArray(new String[cfs.size()]));
	}

	/* 
	 * save data if insert return 'true' else if update return 'false'
	 * 
	 * (non-Javadoc)
	 * @see com.sdata.db.BaseDao#save(java.util.Map)
	 */
	public boolean save(Map<String,Object> data){
		if(data == null){
			return false;
		}
		Object objectId = data.remove(Constants.OBJECT_ID);
		if(objectId == null){
			if(StringUtils.isEmpty(daoCollection.getPrimaryKey())){
				throw new RuntimeException("the property _id of object id is empty and no referce primaryKey!");
			}
			objectId = data.get(daoCollection.getPrimaryKey());
		}
		if(objectId == null){
			throw new RuntimeException("the map data row key is empty !");
		}
		// remove field deal
		this.removeFields(data);
		return this.save(objectId,data);
	}
	
	protected boolean save(Object _id ,Map<String,Object> data){
		boolean insert = true;
		// string to bytes for row key
		if(isExists(_id)){
			insert = false; 
			data.remove(Constants.FETCH_TIME);
		}
		// save main data
		Map<String, Map<String, Object>> multiColflyData = getMultiColflyData(data);
		this.client.saveMultiFamily(daoCollection.getName(),_id, multiColflyData);
		return insert;
	}
	
	/**
	 * multi column family data parse
	 * 
	 * @param data
	 * @return
	 */
	protected Map<String,Map<String,Object>> getMultiColflyData(Map<String,Object> data){
		Map<String,Map<String,Object>>  result = new HashMap<String, Map<String,Object>>();
	    Iterator<ColumnFamily> iterator = daoCollection.getColflys().iterator();
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
		Iterator<String> iterator = daoCollection.getRemove().iterator();
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
		return client.exists(collectionName, id);
	}
	
	/**
	 * delete
	 * 
	 * @param count
	 * @return
	 */
	public void delete(Object id){
		client.delete(collectionName, id);
	}
}
