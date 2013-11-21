package com.sdata.core.data.dao;

import java.util.Map;
import java.util.UUID;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.nus.next.db.Constants;
import com.nus.next.db.hbase.thrift.HBaseClient;

/**
 * @author zhufb
 *
 */
public class HBaseStoreWithGenID extends HBaseStore {
	
	private HBaseGenID hbaseGenID;
	
	public HBaseStoreWithGenID(HBaseClient client,StoreCollection storeCollection){
		super(client,storeCollection);
		this.hbaseGenID = new HBaseGenID(storeCollection.getPkTable(),client);
	}

	@Override
	protected void save(Object _id,Map<String,Object> data){
		String oi = String.valueOf(data.remove(Constants.OBJECT_INDEX));
		// string to bytes for row key
		UUID uuid = UUIDUtils.decode(_id.toString());
		// exists
		// Get row key through Id Generator
		Long rk = hbaseGenID.getOrCreateRowKey(uuid);
		// Index collection save
		hbaseGenID.save(uuid, rk, oi);
		// main data save
		if(isExists(rk)){
			data.remove(com.sdata.core.Constants.FETCH_TIME);
		}else{
			// replace string parent id to long
			this.setParentID(data);
			statisStore.increase(storeCollection.getName());
		}

		// save
		Map<String, Map<String, Object>> multiColflyData = getMultiColflyData(data);
		this.client.saveMultiFamily(storeCollection.getName(),rk, multiColflyData);
	}
	
	protected void setParentID(Map<String,Object> data){
		if(!data.containsKey(Constants.OBJECT_PARENT_ID)){
			return;
		}
		String strpid = StringUtils.valueOf(data.remove(Constants.OBJECT_PARENT_ID));
		UUID uuid = UUIDUtils.decode(strpid);
		if(uuid == null){
			return;
		}
		Long pid = hbaseGenID.getOrCreateRowKey(uuid);
		data.put(Constants.OBJECT_PARENT_ID, pid);
	}
	
	
	public HBaseGenID getHbaseGenID() {
		return hbaseGenID;
	}
}
