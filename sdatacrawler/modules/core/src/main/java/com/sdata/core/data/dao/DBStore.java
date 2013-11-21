package com.sdata.core.data.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.nus.next.config.NSouce;
import com.nus.next.config.SourceConfig;
import com.nus.next.db.NextSourceDao;
import com.nus.next.db.NextSourceDbManager;
import com.nus.next.db.hbase.thrift.HBaseClient;
import com.nus.next.db.hbase.thrift.HBaseClientFactory;
import com.sdata.core.Configuration;
import com.sdata.core.Constants;
import com.sdata.core.parser.html.config.StoreConfig;

/**
 * @author zhufb
 *
 */
public abstract class DBStore {
	
	private static Map<Configuration,Map<String,DBStore>> storeMap = new HashMap<Configuration,Map<String,DBStore>>();
	
	/**
	 * @param data
	 */
	public abstract void save(Map<String,Object> data);
	
	/**
	 * exists
	 * 
	 * @param count
	 * @return
	 */
	public abstract boolean isExists(Object id);
	
	/**
	 * delete
	 * 
	 * @param count
	 * @return
	 */
	public abstract void delete(Object id);
	
	/**
	 * @param conf
	 * @return
	 */
	public static Map<String,DBStore> getStoreMap(Configuration conf){
		if(!storeMap.containsKey(conf)){
			synchronized (DBStore.class) {
				if(!storeMap.containsKey(conf)){
					String dbType = conf.get("DBType");
					if("MongoDB".equals(dbType)){
						String sName = conf.get(Constants.SOURCE);
						if(StringUtils.isEmpty(sName)){
							sName = conf.get("crawlName");
						}
						storeMap.put(conf, getMongoStoreMap(conf,sName));
					}else if("HBase".equals(dbType)){
						storeMap.put(conf, getHBaseStoreMap(conf));
					}
				}
			}
		}
		return storeMap.get(conf);
	}
	
	private static Map<String,DBStore> getMongoStoreMap(Configuration conf,String sName){
		Map<String,DBStore> map = new HashMap<String,DBStore>();
		NSouce source = SourceConfig.getInstance().getSource(sName);
		NextSourceDao sourceDao = NextSourceDbManager.getSourceDao(source);
		Iterator<StoreCollection> collections = StoreConfig.getInstance(conf).getCollections();	
		while(collections.hasNext()){
			StoreCollection collection = collections.next();
			map.put(collection.getName(), new MongoStore(sourceDao,collection));
		}
		return map;
		
	}
	
	private static Map<String,DBStore> getHBaseStoreMap(Configuration conf){
		Map<String,DBStore> map = new HashMap<String,DBStore>();
		String clusterName = conf.get("hbase.cluster.name");
		String namespace = conf.get("hbase.namespace");
		HBaseClient client = null;
		if("next".equals(namespace)){
			client = HBaseClientFactory.getClientWithNormalSeri(clusterName,namespace);
		}else{
			client = HBaseClientFactory.getClientWithCustomSeri(clusterName,namespace);
		}
		Iterator<StoreCollection> collections = StoreConfig.getInstance(conf).getCollections();	
		while(collections.hasNext()){
			StoreCollection collection = collections.next();
			HBaseStore store = null;
			if(StringUtils.isEmpty(collection.getPkTable())){
				store = new HBaseStore(client,collection);
			}else{
				store = new HBaseStoreWithGenID(client,collection);
			}
			map.put(collection.getName(), store);
		}
		return map;
	}
}
