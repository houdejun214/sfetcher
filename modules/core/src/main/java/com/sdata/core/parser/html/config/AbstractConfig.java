package com.sdata.core.parser.html.config;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.io.SAXReader;

import com.sdata.core.Configuration;
import com.sdata.core.util.ApplicationResourceUtils;

/**
 * @author zhufb
 *
 */
public abstract class AbstractConfig{
	
	protected Configuration conf;
	protected String config;
	protected String CONF_XML;
	protected AbstractConfig(String str) {
		this.load(str);
	}
	
	protected AbstractConfig(Configuration conf){
		this.conf = conf;
		this.load(conf.get(getConfXmlKey()));
	}

	protected abstract void parse(Document document);
	
	protected abstract String getConfXmlKey();
	
	protected void load(String str){
		if(StringUtils.isEmpty(str)){
			throw new RuntimeException("load crawler file error,file is null!");
		}
		Document document = null;
		try {
			document = this.getDocFromStr(str);
		} catch (DocumentException e) {
			try {
				document = this.getDocFromPath(str);
			} catch (DocumentException e1) {
				throw new RuntimeException("load crawler file exception"+str,e1);
			}
		}
		this.parse(document);
	}
	
	protected Document getDocFromPath(String path) throws DocumentException{
		String file = ApplicationResourceUtils.getResourceUrl(path);
		SAXReader reader = new SAXReader();
		Document document = reader.read(file);
		return document;
	}
	
	protected Document getDocFromStr(String str) throws DocumentException{
		 return DocumentHelper.parseText(str);  
	}
	
}