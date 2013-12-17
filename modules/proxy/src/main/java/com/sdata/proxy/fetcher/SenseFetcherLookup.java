package com.sdata.proxy.fetcher;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.lakeside.core.utils.ClassUtils;

/**
 * @author zhufb
 *
 */
public class SenseFetcherLookup {

	private final static String NAME = "FID";
	
	private static  Map<String, Class<? extends SenseFetcher>> fetcherMap = new HashMap<String,Class<? extends SenseFetcher>>();
	
	public static Map<String, Class<? extends SenseFetcher>> getSubClassMap(){
		if(fetcherMap.size() == 0){
			synchronized (fetcherMap) {
				if(fetcherMap.size() ==0){
					Set<Class<? extends SenseFetcher>> subClass = ClassUtils.getSubClass(SenseFetcher.class, "com.sdata");
					for(Class<? extends SenseFetcher> sub:subClass){
						String fid = getFID(sub); 
						fetcherMap.put(fid,sub);
					}
				}
			}
		}
		return fetcherMap;
	}
	
	private static String getFID(Class<?> cls){
		try {
			return cls.getField(NAME).get(null).toString();
		} catch (Exception e) {
			throw new RuntimeException("No "+ NAME + " field exists in class"+cls,e);
		}
	}

}
