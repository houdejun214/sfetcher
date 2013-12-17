package com.sdata.proxy.resource;

import com.sdata.context.config.CrawlAppContext;
import com.sdata.core.resource.Resource;
import com.sdata.core.resource.ResourceDB;
import com.sdata.core.resource.ResourceFactory;

/**
 * @author zhufb
 *
 */
public class Resources<T extends Resource> {

	public static Resources<TencentResource> Tencent = new Resources<TencentResource>("tencent",TencentResource.class);
	public static Resources<WeiboResource> Weibo = new Resources<WeiboResource>("weibo",WeiboResource.class);
	private static ResourceDB db;
	private String name;
	private ResourceFactory<T> factory;
	private Resources(String name,Class<T> cls){
		this.name = name;
		this.factory = new ResourceFactory<T>(name,getResourceDB(),cls);
	}
	
	public String getName() {
		return name;
	}
	
	public T get(){
		return factory.getResource();
	}
	
	private static ResourceDB getResourceDB(){
		if(db == null){
			synchronized(Resources.class){
				if(db == null){
					db = new ResourceDB(CrawlAppContext.conf);
				}
			}
		}
		return db;
	}
	
	
}
