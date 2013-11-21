package com.sdata.hot.store;

import org.springframework.beans.factory.FactoryBean;

import com.nus.next.db.hbase.thrift.HBaseClient;
import com.nus.next.db.hbase.thrift.HBaseClientFactory;
import com.sdata.core.Configuration;

/**
 * 
 * HbaseClient 构造类，主要用于Spring的依赖注入支持
 * 
 * @author houdejun
 *
 */
public class HBaseClientFactoryBean {
	
	private static final Object sync = new Object();
	private static HBaseClient client = null;
	
	private static void initHbaseClient(Configuration conf){
		synchronized(sync){
			if(client==null){
				String clusterName = conf.get("hbase.cluster.name");
				String namespace = conf.get("hbase.namespace");
				client = HBaseClientFactory.getClientWithCustomSeri(clusterName, namespace);
			}
		}
	}
	
	public static HBaseClient getObject(Configuration conf)  {
		if(client==null){
			initHbaseClient(conf);
		}
		return client;
	}
}
