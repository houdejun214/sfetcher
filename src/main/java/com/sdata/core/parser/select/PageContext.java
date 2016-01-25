package com.sdata.core.parser.select;

import com.lakeside.config.Configuration;
import org.jsoup.nodes.Document;

import java.util.HashMap;
import java.util.Map;

public class PageContext {
    private Configuration conf;
	private Document doc;
	private Map<String,String> httpHeader = new HashMap<String,String>();
	private Map<String,Object> metadata = new HashMap<String,Object>();
	private Map<String,Object> contextVariable = new HashMap<String,Object>();
	
	public PageContext(Configuration conf){
		this.conf = conf;
	}
	public PageContext(Configuration conf, Document doc){
		this.conf = conf;
		this.doc = doc;
	}

	public Map<String, Object> getMetadata() {
		return metadata;
	}

	public void setMetadata(Map<String, Object> metadata) {
		this.metadata = metadata;
	}
	
	public void addData(String k,Object v){
		this.metadata.put(k, v);
	}
	
	public boolean containData(String k){
		return this.metadata.containsKey(k);
	}

	public Document getDoc() {
		return doc;
	}

    public Map<String, Object> getContextVariable() {
        return contextVariable;
    }

    /**
	 * get the variable value by variable key,
	 * the variable may contained in metadata, contextVariable or configuration 
	 * @param key
	 * @return
	 */
	public Object getVariable(String key){
		if(this.metadata!=null&&this.metadata.containsKey(key)){
			return this.metadata.get(key);
		}else if(this.contextVariable!=null&&this.contextVariable.containsKey(key)){
			return this.contextVariable.get(key);
		}else if(this.conf.containsKey(key)) {
            return this.conf.get(key);
		}else {
		}
		return null;
	}

	public Document getDocument() {
		return doc;
	}
	
	public boolean hasVariable(String key) {
		if(this.metadata!=null&&this.metadata.containsKey(key)){
			return true;
		}else if(this.contextVariable!=null&&this.contextVariable.containsKey(key)){
			return true;
		}else if(this.conf.containsKey(key)){
			return true;
		}
		return false;
	}
	
	public void putVariable(String key, Object value) {
		this.contextVariable.put(key, value);
	}

    public void putVariableAll(Map<String, Object> maps) {
        this.contextVariable.putAll(maps);
    }

    public void putData(String key, Object value) {
		this.metadata.put(key, value);
	}
	
	public Object getField(String name){
		return getVariable(name);
	}

	public Configuration getConfig() {
		return conf;
	}

	public void setDocument(Document doc) {
		this.doc = doc;
	}

	public Map<String, String> getHttpHeader() {
		return httpHeader;
	}

	public void setHttpHeader(Map<String, String> httpHeader) {
		this.httpHeader = httpHeader;
	}
	
	public void addHttpHeader(String key,String val){
		this.httpHeader.put(key, val);
	}
	
}