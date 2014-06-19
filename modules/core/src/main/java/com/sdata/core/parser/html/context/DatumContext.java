package com.sdata.core.parser.html.context;

import java.util.HashMap;
import java.util.Map;

import org.jsoup.nodes.Document;

import com.sdata.context.config.Configuration;
import com.sdata.context.parser.IParserContext;
import com.sdata.core.parser.config.DatumConfig;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.datum.DatumField;

/**
 * @author zhufb
 *
 */
public class DatumContext implements IParserContext {
	private Configuration conf;
	private Document doc;
	private Map<String,String> httpHeader = new HashMap<String,String>();
	private Map<String,Object> metadata = new HashMap<String,Object>();
	private Map<String,Object> contextVariable = new HashMap<String,Object>();
	
	public DatumContext(Configuration conf){
		this.conf = conf;
	}
	public DatumContext(Configuration conf,Document doc){
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
		}else if(this.conf.containsKey(key)){
			return this.conf.get(key);
		}else {
			Field field = DatumConfig.getInstance(conf).getField(this.doc, key);
			if(field != null){
				this.metadata.put(key, field.getData(this, doc));
				return this.metadata.get(key);
			}
		}
		return null;
	}
	
	public Object getTransValue(String key,Map<String,Object> data){
		if(this.metadata!=null&&this.metadata.containsKey(key)){
			return this.metadata.get(key);
		}else if(this.contextVariable!=null&&this.contextVariable.containsKey(key)){
			return this.contextVariable.get(key);
		}else if(this.conf.containsKey(key)){
			return this.conf.get(key);
		}else {
			DatumField field = (DatumField)DatumConfig.getInstance(conf).getField(this.doc, key);
			if(field != null){
				this.metadata.put(key, field.transData(this, data));
				return this.metadata.get(key);
			}
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
	
	private DatumConfig getDatumConfig() {
		return DatumConfig.getInstance(conf);
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
		if(!this.hasVariable(name)){
			Field field = getDatumConfig().getField(this.doc, name);
			Object data = field.getData(this, doc);
			metadata.put(name, data);
		}
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
