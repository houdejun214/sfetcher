package com.sdata.core.data.dao;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.framework.db.hbase.Constants;
import com.framework.db.hbase.ids.IdGenerator;
import com.framework.db.hbase.thrift.BytesBufferUtils;
import com.framework.db.hbase.thrift.HBaseClient;
import com.lakeside.core.utils.UUIDUtils;

/**
 * @author zhufb
 *
 */
public class HBaseGenID{
	
	private String name ;
	private HBaseClient client;
	
	public HBaseGenID(String name,HBaseClient client){
		this.name = name;
		this.client = client;
		this.client.createTable(name, Constants.HBASE_DEFAULT_COLUMN_FAMILY);
	}
	
	private Map<String,Object> getIndexData(Long rk,String column){
		Map<String,Object>  result = new HashMap<String,Object>();
		result.put(Constants.HBASE_INDEX_ROWKEY, rk);
		result.put(column, true);
		return result;
	}
	
	public Long queryRowKey(UUID id){
		Map<String, Object> result = client.query(name, BytesBufferUtils.buffer(id),getFullColName(Constants.HBASE_INDEX_ROWKEY));
		if(result == null||result.size() == 0){
			return null;
		}
		return (Long)result.get(Constants.HBASE_INDEX_ROWKEY);
	}
	
	public Long getOrCreateRowKey(UUID id){
		Long rk = queryRowKey(id);
		if(rk == null) {
			if(lock(id)){
				rk = queryRowKey(id);
				if(rk == null){
					rk = preGeneratorRkAndSave(id);
				}
				unlock(id);
			}
		}
		return rk;
	}
	
	private boolean lock(UUID id){
		String path = String.valueOf(id.hashCode());
		int maxwait = 120*1000;
		int wait = 0;
		while(!client.getDbLock().lock(path)){
			try {
				Thread.sleep(1000);
				wait+=1000;
				if(wait >= maxwait){
					this.unlock(id);
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return true;
	}

	private void unlock(UUID id){
		client.getDbLock().release(String.valueOf(id.hashCode()));
	}
	
	public void save(UUID id,Long rk,String column){
		if(isExists(id,column)){
			return;
		}
		client.save(name, id, getIndexData(rk,column));
	}

	public boolean isExists(String id, String column) {
		return isExists(UUIDUtils.decode(id), column);
	}
	
	private Long preGeneratorRkAndSave(UUID id){
		long rowkey = IdGenerator.generator().getNextId();
		Map<String,Object> map = new HashMap<String,Object>();
		map.put(Constants.HBASE_INDEX_ROWKEY, rowkey);
		client.save(name, id, map);
		return rowkey;
	}
	
	/**
	 * select exists
	 * 
	 * @param count
	 * @return
	 */
	public boolean isExists(UUID id){
		return client.exists(name,id);
	}
	
	/**
	 * select exists
	 * 
	 * @param count
	 * @return
	 */
	public boolean isExists(UUID id,String column){
		return client.exists(name, BytesBufferUtils.buffer(id), getFullColName(column));
	}

	private String getFullColName(String column){
		return Constants.HBASE_DEFAULT_COLUMN_FAMILY.concat(":").concat(column);
	}
	
	/**
	 * delete
	 * 
	 * @return
	 */
	public void delete(UUID id){
		if(id == null){
			return;
		}
		client.delete(name, id);
	}
}
