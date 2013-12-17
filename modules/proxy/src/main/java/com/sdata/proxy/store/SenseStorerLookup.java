package com.sdata.proxy.store;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.lakeside.core.utils.ClassUtils;

/**
 * @author zhufb
 *
 */
public class SenseStorerLookup {

	private final static String NAME = "SID";

	private static  Map<String, Class<? extends SenseStorer>> stroreMap = new HashMap<String,Class<? extends SenseStorer>>();
	
	public static Map<String, Class<? extends SenseStorer>> getSubClassMap(){
		if(stroreMap.size() == 0){
			synchronized (stroreMap) {
				if(stroreMap.size() ==0){
					Set<Class<? extends SenseStorer>> subClass = ClassUtils.getSubClass(SenseStorer.class, "com.sdata");
					for(Class<? extends SenseStorer> sub:subClass){
						String sid = getSID(sub); 
						stroreMap.put(sid,sub);
					}
					stroreMap.put(SenseStorer.DEFAULT_SENSE_STORER, SenseStorer.class);
				}
			}
		}
		return stroreMap;
	}
	
	private static String getSID(Class<?> cls){
		try {
			return cls.getField(NAME).get(null).toString();
		} catch (Exception e) {
			throw new RuntimeException("No "+ NAME + " field exists in class"+cls,e);
		}
	}
}
