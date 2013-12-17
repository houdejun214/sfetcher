package com.sdata.core.resource;

import java.util.Map;

import net.sf.json.JSONObject;

import org.apache.commons.collections.MapUtils;

/**
 * Resource dto
 * 
 * @author zhufb
 *
 */
public class Resource {

	public Resource(Map<String,Object> map){
		this.id = MapUtils.getLong(map, "id");
		this.source = MapUtils.getString(map, "source");
		this.type = MapUtils.getString(map, "type");
		this.value = MapUtils.getString(map, "value");
		this.data = JSONObject.fromObject(this.value);
	}
	
	private Long id;
	private String source;
	private String type;
	private String value;
	private Map<String,String> data;
	
	public Long getId() {
		return id;
	}
	public String getSource() {
		return source;
	}
	public String getType() {
		return type;
	}
	public String getValue() {
		return value;
	}
	
	public Map<String, String> get() {
		return data;
	}

	public String get(String key) {
		return data.get(key);
	}
	
	public boolean equals(Resource r){
		if(r == null){
			return false;
		}
		return getId().equals(r.getId());
	}
	
}
