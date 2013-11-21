package com.sdata.core.data;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * web site type
 * 
 * @author houdj
 *
 */
public class DataWebSiteType {
	
	/**
	 * flickr site
	 */
	public static final String Flickr ="flickr";
	/**
	 * picasa site
	 */
	public static final String Picasa ="picasa";
	/**
	 * panoramio site
	 */
	public static final String Panoramio = "panoramio";
	/**
	 * instagram site
	 */
	public static final String Instagram ="instagram";
	
	public static List<String> getList(){
		return Arrays.asList(new String[]{
				Flickr,
				Picasa,
				Panoramio,
				Instagram
		});
	}
	
	public static Map<String,String> getMap(){
		Map<String,String> maps = new HashMap<String, String>();
		maps.put(Flickr,"Flickr");
		maps.put(Picasa,"Picasa");
		maps.put(Panoramio,"Panoramio");
		maps.put(Instagram,"Instagram");
		return maps;
	}
}
