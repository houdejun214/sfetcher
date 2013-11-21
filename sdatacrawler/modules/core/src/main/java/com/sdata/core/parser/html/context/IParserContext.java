package com.sdata.core.parser.html.context;

import java.util.Map;

import org.jsoup.nodes.Document;

import com.sdata.core.Configuration;


/**
 * @author zhufb
 *
 */
public interface IParserContext {

	public boolean hasVariable(String key);
	
	public Object getVariable(String key);
	
	public void addData(String k,Object v);
	
	public Document getDocument();
	
	public Configuration getConfig();
	
	public Object getField(String name);

	public void putVariable(String key, Object value);
	
	public Map<String,String> getHttpHeader();
	
	public void setDocument(Document doc);

}