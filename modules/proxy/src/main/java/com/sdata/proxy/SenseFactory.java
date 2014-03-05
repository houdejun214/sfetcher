package com.sdata.proxy;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import com.sdata.context.config.Configuration;
import com.sdata.context.config.CrawlAppContext;
import com.sdata.context.state.RunState;
import com.sdata.proxy.fetcher.SenseFetcher;
import com.sdata.proxy.fetcher.SenseFetcherLookup;
import com.sdata.proxy.fetcher.SingletonFetcher;
import com.sdata.proxy.store.SenseStorer;
import com.sdata.proxy.store.SenseStorerLookup;

/**
 * sense factory includes fetcher and store
 * 
 * @author zhufb
 *
 */
public final class SenseFactory {

	private static Map<String,Class<? extends SenseFetcher>> fetcherClassMap = SenseFetcherLookup.getSubClassMap();
	
	private static Map<String, Class<? extends SenseStorer>> storeClassMap = SenseStorerLookup.getSubClassMap();

	private static Map<String,SenseStorer> storersMap = new HashMap<String,SenseStorer>();
	
	private static Map<String,SenseFetcher> globalFetchers = new HashMap<String,SenseFetcher>();
	
	private static ThreadLocal<Map<String,SenseFetcher>> localFetchers = new ThreadLocal<Map<String,SenseFetcher>>();

	public static SenseFetcher getFetcher(String id) {
		Map<String, SenseFetcher> pool = localFetchers.get();
		if(pool == null){
			pool = new HashMap<String,SenseFetcher>();
			localFetchers.set(pool);
		}
		SenseFetcher senseFetcher = pool.get(id);
		if(senseFetcher == null){
			Class<? extends SenseFetcher> cls = fetcherClassMap.get(id);
			if(cls == null){
				return null;
			}
			boolean isSingletonFetcher = SingletonFetcher.class.isAssignableFrom(cls);
			if(isSingletonFetcher){
				if(globalFetchers.get(id) == null){
					synchronized(globalFetchers){
						if(globalFetchers.get(id) == null){
							globalFetchers.put(id, newFetcher(id, cls));
						}
					}
				}
				senseFetcher = globalFetchers.get(id);
			}else{
				senseFetcher = newFetcher(id, cls);
			}
		}
		pool.put(id, senseFetcher);
		return senseFetcher;
	}
	
	public static SenseStorer getStorer(String id) {
		Class<? extends SenseStorer> cls = storeClassMap.get(id);
		if(cls == null){
			cls = storeClassMap.get(SenseStorer.DEFAULT_SENSE_STORER);
		}
		
		if(storersMap.get(id) == null){
			synchronized(storersMap){
				if(storersMap.get(id) == null){
					storersMap.put(id, newStorer(id,cls));
				}
			}
		}
		return storersMap.get(id);
	}
	
	private static Object newInstance(String id,Class cls){
		try {
			Constructor constructor = cls.getConstructor(Configuration.class,RunState.class);
			Object newInstance = constructor.newInstance(SenseConfig.getConfig(id),getRunState());
			return newInstance;
		} catch (Exception e) {
			throw new RuntimeException("new instance "+cls,e);
		}
	}

	private static SenseStorer newStorer(String id,Class<? extends SenseStorer> cls){
		return (SenseStorer)newInstance(id,cls);
	}
	
	private static SenseFetcher newFetcher(String id,Class<? extends SenseFetcher> cls){
		return (SenseFetcher)newInstance(id,cls);
	}

	private static RunState getRunState(){
		return CrawlAppContext.state;
	}
	
}
