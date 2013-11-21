package com.sdata.core;

import java.util.HashMap;
import java.util.Map;

import com.lakeside.core.utils.StringUtils;

/**
 * 
 * represent the crawl data
 * 
 * @author houdejun
 *
 */
public class FetchDatum {
	private String url;
	private String localFilePath;
	protected Map<String, Object> metadata = new HashMap<String,Object>();
	private Object id;
	private String name;
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	// keep the current information of the datum;
	private String current;
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getLocalFilePath() {
		return localFilePath;
	}
	public void setLocalFilePath(String localFilePath) {
		this.localFilePath = localFilePath;
	}
	public Map<String, Object> getMetadata() {
		return metadata;
	}
	public void setMetadata(Map<String, Object> matadata) {
		if(matadata!=null){
			if(!matadata.containsKey("url")){
				matadata.put("url", url);
			}
		}
		this.metadata.putAll(matadata);
	}
	
	public void clearMetadata() {
		if(this.metadata!=null){
			this.metadata=null;
		}
	}
	
	
	public void addMetadata(String key,Object value){
		if(this.metadata==null){
			this.metadata = new HashMap<String,Object>();
		}
		this.metadata.put(key, value);
	}
	
	public void addAllMetadata(Map<String, ?> map){
		if(this.metadata==null){
			this.metadata = new HashMap<String,Object>();
		}
		this.metadata.putAll(map);
	}
	
	public Object getId() {
		return id;
	}
	public void setId(Object id) {
		this.id = id;
	}
	public String getCurrent() {
		return current;
	}
	public void setCurrent(String current) {
		this.current = current;
	}
	
	public boolean getMetaBoolean(String key,boolean defaultValue){
		if(metadata!=null && metadata.containsKey(key)){
			String val = StringUtils.valueOf(this.metadata.get(key));
			return Boolean.parseBoolean(val);
		}
		return defaultValue;
	}
	
	public String getMeta(String key){
		if(metadata!=null && metadata.containsKey(key)){
			String val = StringUtils.valueOf(this.metadata.get(key));
			return val;
		}
		return "";
	}
}
