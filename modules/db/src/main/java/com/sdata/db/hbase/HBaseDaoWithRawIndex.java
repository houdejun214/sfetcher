package com.sdata.db.hbase;

import java.util.Map;
import java.util.UUID;

import com.framework.db.hbase.Constants;
import com.framework.db.hbase.thrift.HBaseClient;
import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.UUIDUtils;
import com.sdata.db.DaoCollection;

/**
 * @author zhufb
 *
 */
public class HBaseDaoWithRawIndex extends HBaseDao {
	
	private HBaseRawIndex hbaseGenID;
	
	public HBaseDaoWithRawIndex(HBaseClient client,DaoCollection daoCollection){
		super(client,daoCollection);
		this.hbaseGenID = new HBaseRawIndex(daoCollection.getPkTable(),client);
	}

	@Override
	protected boolean save(Object _id,Map<String,Object> data){
		boolean insert = true;
		String oi = String.valueOf(data.remove(Constants.OBJECT_INDEX));
		// string to bytes for row key
		// exists
		// Get row key through Id Generator
		Long rk = hbaseGenID.getOrCreateRowKey(_id);
		
		// Index collection save
		hbaseGenID.save(_id, rk, oi);
		
		// main data save
		if(isExists(rk)){
			data.remove(com.sdata.context.config.Constants.FETCH_TIME);
			insert = false;
		}else{
			// replace string parent id to long
			this.setParentID(data);
		}

		// save
		Map<String, Map<String, Object>> multiColflyData = getMultiColflyData(data);
		this.client.saveMultiFamily(daoCollection.getName(),rk, multiColflyData);
		return insert;
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
	
	
	public HBaseRawIndex getHbaseGenID() {
		return hbaseGenID;
	}
}
