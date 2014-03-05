package com.sdata.core.data.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.sdata.context.config.Configuration;
import com.sdata.core.parser.config.StoreConfig;
import com.sdata.db.BaseDao;
import com.sdata.db.DaoCollection;
import com.sdata.db.hbase.HBaseDao;
import com.sdata.db.hbase.HBaseDaoWithRawIndex;

/**
 * @author zhufb
 *
 */
public class DaoFactory {
	
	private static Map<Configuration,Map<String,BaseDao>> daos = new HashMap<Configuration,Map<String,BaseDao>>();

	/**
	 * get daos with conf
	 * 
	 * @param conf
	 * @return
	 */
	public static Map<String,BaseDao> getDaos(Configuration conf){
		if(!daos.containsKey(conf)){
			synchronized (BaseDao.class) {
				if(!daos.containsKey(conf)){
					daos.put(conf, getHBaseDaos(conf));
				}
			}
		}
		return daos.get(conf);
	}
	
	private static Map<String,BaseDao> getHBaseDaos(Configuration conf){
		Map<String,BaseDao> map = new HashMap<String,BaseDao>();
		String clusterName = conf.get("hbase.cluster.name");
		String namespace = conf.get("hbase.namespace");
		HBaseClient client = HBaseClientFactory.getClientWithCustomSeri(clusterName,namespace);
		Iterator<DaoCollection> collections = StoreConfig.getInstance(conf).getCollections();	
		while(collections.hasNext()){
			DaoCollection collection = collections.next();
			HBaseDao store = null;
			if(StringUtils.isEmpty(collection.getPkTable())){
				store = new HBaseDao(client,collection);
			}else{
				store = new HBaseDaoWithRawIndex(client,collection);
			}
			map.put(collection.getName(), store);
		}
		return map;
	}
}
