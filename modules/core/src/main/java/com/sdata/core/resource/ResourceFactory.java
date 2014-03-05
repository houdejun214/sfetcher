package com.sdata.core.resource;

import java.lang.reflect.Constructor;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Resource dto
 * 
 * @author zhufb
 *
 */
public class ResourceFactory<T extends Resource> {
	
	protected static final Logger log = LoggerFactory.getLogger("SdataCrawler.ResourceFactory");
	private static Object syn = new Object();
	private String source ;
	private ResourceDB rdb ;
	private Class<T> cls;
	private T global;
	private ThreadLocal<T> local = new ThreadLocal<T>(); 
	
	public ResourceFactory(String source,ResourceDB rdb,Class<T> cls){
		this.source = source;
		this.rdb = rdb;
		this.cls = cls;
	}

	public T getResource(){
		return getResource(null);
	}
	
	public T getResource(String type){
		T t = local.get();
		if(global == null||global.equals(t)){
			synchronized(syn){
				if(global==null||global.equals(t)){
					//if global is not null, must return to pool for next cicle use
					if(global!=null){
						returnResource(global);
					}
					global = newResource(type);
				}
			}
		}
		// dont need clone
		local.set(global);
		return local.get();
	}
	
	private T newResource(String type){
		Map<String, Object> map   = null;
		while(map == null){
			map = rdb.getResource(source,type);
			if(map == null){
				log.error(" source :"+ source + ",type:"+ type +"no resource can be used please add !" );
				await(10000);
				continue;
			}
		}
		try {
			Constructor constructor = cls.getConstructor(Map.class);
			T t = (T) constructor.newInstance(map);
			this.setUsing(t);
			return t;
		} catch (Exception e) {
			return null;
		}
	}

	private void setUsing(T resource){
		rdb.updateUsing(resource.getId());
	}
	
	private void returnResource(T resource){
		rdb.returnResource(resource.getId());
	}
	
	private void await(long millis){
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
}
