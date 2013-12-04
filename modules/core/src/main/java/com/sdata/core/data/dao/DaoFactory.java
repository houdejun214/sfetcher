package com.sdata.core.data.dao;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.lang.StringUtils;

import com.framework.config.NSouce;
import com.framework.config.SourceConfig;
import com.framework.db.hbase.thrift.HBaseClient;
import com.framework.db.hbase.thrift.HBaseClientFactory;
import com.framework.db.mongo.NextSourceDao;
import com.framework.db.mongo.NextSourceDbManager;
import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.core.parser.config.StoreConfig;
import com.sdata.db.BaseDao;
import com.sdata.db.HBaseDao;
import com.sdata.db.HBaseDaoWithRawIndex;
import com.sdata.db.MongoDao;
import com.sdata.db.DaoCollection;

/**
 * @author zhufb
 *
 */
public class DaoFactory {
	
	private static Map<Configuration,Map<String,BaseDao>> daos = new HashMap<Configuration,Map<String,BaseDao>>();

	/**
	 * @param conf
	 * @return
	 */
	public static Map<String,BaseDao> getDaos(Configuration conf){
		if(!daos.containsKey(conf)){
			synchronized (BaseDao.class) {
				if(!daos.containsKey(conf)){
					String dbType = conf.get("DBType");
					if("MongoDB".equals(dbType)){
						String sName = conf.get(Constants.SOURCE);
						if(StringUtils.isEmpty(sName)){
							sName = conf.get("crawlName");
						}
						daos.put(conf, getMongoDaos(conf,sName));
					}else if("HBase".equals(dbType)){
						daos.put(conf, getHBaseDaos(conf));
					}
				}
			}
		}
		return daos.get(conf);
	}
	
	private static Map<String,BaseDao> getMongoDaos(Configuration conf,String sName){
		Map<String,BaseDao> map = new HashMap<String,BaseDao>();
		NSouce source = SourceConfig.getInstance().getSource(sName);
		NextSourceDao sourceDao = NextSourceDbManager.getSourceDao(source);
		Iterator<DaoCollection> collections = StoreConfig.getInstance(conf).getCollections();	
		while(collections.hasNext()){
			DaoCollection collection = collections.next();
			map.put(collection.getName(), new MongoDao(sourceDao,collection));
		}
		return map;
		
	}
	
	private static Map<String,BaseDao> getHBaseDaos(Configuration conf){
		Map<String,BaseDao> map = new HashMap<String,BaseDao>();
		String clusterName = conf.get("hbase.cluster.name");
		String namespace = conf.get("hbase.namespace");
		HBaseClient client = null;
		if("next".equals(namespace)){
			client = HBaseClientFactory.getClientWithNormalSeri(clusterName,namespace);
		}else{
			client = HBaseClientFactory.getClientWithCustomSeri(clusterName,namespace);
		}
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
