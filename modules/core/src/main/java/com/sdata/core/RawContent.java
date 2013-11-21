package com.sdata.core;

import java.util.HashMap;
import java.util.Map;

public class RawContent {
	
	public RawContent(String content){
		this.content = content;
	}
	
	public RawContent(String url,String content){
		this.url = url;
		this.content = content;
	}
	
	private String url;
	
	private String content;
	
	private Map<String,Object> metadata = null;

	public String getContent() {
		return content;
	}

	public void setContent(String content) {
		this.content = content;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public void setMetadata(String key,Object value){
		if(this.metadata==null){
			this.metadata = new HashMap<String,Object>();
		}
		this.metadata.put(key, value);
	}
	
	public void addAllMeata(Map<String,Object> map){
		if(this.metadata==null){
			this.metadata = new HashMap<String,Object>();
		}
		this.metadata.putAll(map);
	}
	
	public Object getMetadata(String key) {
		if(this.metadata!=null){
			return this.metadata.get(key);
		}
		return null;
	}
	
	public boolean isEmpty(){
		if(this.content==null || "".equals(content)){
			return true;
		}
		return false;
	}
}
