package com.sdata.solr.filter;

import java.util.regex.Matcher;
import java.util.regex.Pattern;


/**
 * @author zhufb
 *
 */
public class SolrDataFilter {
	private static final String GEO = "geo";
	private final String TITLE = "title";
	private final String CONTENT = "content";
	private final String URL = "url";
	static Pattern compile = Pattern.compile("(-?\\d+.?\\d+,-?\\d+.?\\d+)");
	
	//geo order by latitude,longitude. 
	public static Object filter(String key,Object value){
		if(key==null||value==null) {
			return null;
		}else if(GEO.equals(key)){
			return filterGeo(value);
		}
		return value;
	}

	private static Object filterGeo(Object value){
		Matcher matcher = compile.matcher(value.toString());
    	if(!matcher.find()){
    		return null;
    	}
    	return value;
	}
}
