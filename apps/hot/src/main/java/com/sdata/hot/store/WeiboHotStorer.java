package com.sdata.hot.store;

import java.util.Map;

import com.framework.db.hbase.Constants;
import com.framework.db.hbase.thrift.HBaseClient;
import com.sdata.context.config.Configuration;
import com.sdata.context.state.RunState;
import com.sdata.core.FetchDatum;
import com.sdata.core.data.store.SdataBaseStorer;

/**
 * @author zhufb
 *
 */
public class WeiboHotStorer extends SdataBaseStorer {

	private HBaseClient client;
	private String tweetTable;
	private String userTable;
	private String retRelationTable;
	public WeiboHotStorer(Configuration conf, RunState state) {
		super(conf, state);
		this.tweetTable = super.getConf("hbase.tweet.table","tweets");
		this.userTable = super.getConf("hbase.user.table","users");
		this.retRelationTable = super.getConf("hbase.retRelation.table","rets");
		this.client = HBaseClientFactoryBean.getObject(super.getConf());
		this.client.createTable(tweetTable,Constants.HBASE_DEFAULT_COLUMN_FAMILY);
		this.client.createTable(userTable,Constants.HBASE_DEFAULT_COLUMN_FAMILY);
		this.client.createTable(retRelationTable,Constants.HBASE_DEFAULT_COLUMN_FAMILY);
	}
	
	public HBaseClient getClient() {
		return client;
	}

	@Override
	public void save(FetchDatum datum) {
		Map<String, Object> metadata = datum.getMetadata();
		Object user = metadata.remove("user");
		if(user!=null&&user instanceof Map){
			Map map = (Map)user;
			long rk =Long.valueOf(String.valueOf(map.get("id")));
			client.save(this.userTable, rk, map);
		}
		Object rets = metadata.remove("rets");
		if(rets!=null&&rets instanceof Map){
			Map map = (Map)rets;
			long rk =Long.valueOf(String.valueOf(map.get("id")));
			client.save(this.retRelationTable, rk, map);
		}
		long rk = Long.valueOf(String.valueOf(metadata.get("id")));
		client.save(tweetTable, rk, metadata);
	}
	
	public boolean isExists(FetchDatum datum) {
		return false;
	}
}
