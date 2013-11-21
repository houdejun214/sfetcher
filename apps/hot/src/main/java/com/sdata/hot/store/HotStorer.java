package com.sdata.hot.store;

import java.util.Map;

import com.nus.next.db.Constants;
import com.nus.next.db.hbase.thrift.HBaseClient;
import com.sdata.core.Configuration;
import com.sdata.core.FetchDatum;
import com.sdata.core.RunState;
import com.sdata.core.data.SdataStandardStorer;
import com.sdata.hot.HotConstants;

/**
 * @author zhufb
 *
 */
public class HotStorer extends SdataStandardStorer {

	private HBaseClient client;
	private String tableName;
	public HotStorer(Configuration conf, RunState state) {
		super(conf, state);
	}
	
	@Override
	protected void init() {
		this.tableName = super.getConf("hbase.table","senze");
		this.client = HBaseClientFactoryBean.getObject(super.getConf());
		this.client.createTable(tableName,Constants.HBASE_DEFAULT_COLUMN_FAMILY);
	}
	
	public HBaseClient getClient() {
		return client;
	}

	@Override
	public void save(FetchDatum datum) throws Exception {
		Map<String, Object> metadata = datum.getMetadata();
		Object rk = metadata.remove(HotConstants.ROWKEY);
		client.save(tableName, rk, metadata);
	}
	
	public boolean isExists(FetchDatum datum) {
		return false;
	}
}