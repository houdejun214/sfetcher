package com.sdata.solr.handle;

import java.util.HashMap;
import java.util.Map;

/**
 * @author zhufb
 *
 */
public class HandleFactory {
	
	private final static String DEFAULT_HANDLE = "default";
	private static Map<String,IndexDataHandle> handles = new HashMap<String,IndexDataHandle>();
	static{
		handles.put(DEFAULT_HANDLE, new IndexDataHandle());
	}
	
	public static IndexDataHandle getHandle(String site){
		return handles.containsKey(site)?handles.get(site):handles.get(DEFAULT_HANDLE);
	}
}
