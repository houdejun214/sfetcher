package com.sdata.core.parser.html.context;

import java.util.HashMap;
import java.util.Map;

import net.sf.json.JSONObject;

import org.jsoup.nodes.Document;

import com.lakeside.core.utils.MapUtils;
import com.sdata.context.config.Configuration;
import com.sdata.context.parser.IParserContext;

/**
 * @author zhufb
 *
 */
public class StrategyContext implements IParserContext {
	private Configuration conf;
	private Document doc;
	private JSONObject json;
	private Map<String,Object> contextVariable = new HashMap<String,Object>();
    private Map<String,Object> returnContext = new HashMap<String,Object>();
	private Map<String,String> httpHeader = new HashMap<String,String>();
	public StrategyContext(Configuration conf,Document doc){
		this.conf = conf;
		this.doc = doc;
	}
	
	public StrategyContext(Configuration conf,JSONObject json){
		this.conf = conf;
		this.json = json;
	}

	public void addData(String k,Object v) {
        returnContext.put(k, v);
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
		return getField(key);
	}
	
	public Document getDocument() {
		return doc;
	}

	public boolean hasVariable(String key) {
		if(this.contextVariable!=null&&this.contextVariable.containsKey(key)){
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

    public Map<String, Object> getReturnContext() {
        return returnContext;
    }

    public Configuration getConfig() {
		return conf;
	}

	public void setDocument(Document doc) {
		this.doc = doc;
	}

	public Object getField(String name) {
		if(this.contextVariable!=null&&this.contextVariable.containsKey(name)){
			return contextVariable.get(name);
		}else if(this.conf.containsKey(name)){
			return this.conf.get(name);
		}else if(json != null){
			return MapUtils.getInter(json, name);
		}
		return null;
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
