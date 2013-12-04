package com.sdata.hot.store;

import java.util.Map;

import com.framework.db.hbase.Constants;
import com.framework.db.hbase.thrift.HBaseClient;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.data.store.SdataStandardStorer;
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
