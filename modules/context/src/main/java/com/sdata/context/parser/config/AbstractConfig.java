package com.sdata.context.parser.config;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.dom4j.io.SAXReader;

import com.lakeside.core.utils.ApplicationResourceUtils;
import com.sdata.context.config.Configuration;

/**
 *
 *
 */
public abstract class AbstractConfig{
	
	protected Configuration conf;
	protected String config;

	protected AbstractConfig(Configuration conf){
		this.conf = conf;
		this.load(conf.get(getConfXmlKey()));
	}

	protected abstract void load(Document document);
	
	protected abstract String getConfXmlKey();
	
	protected void load(String str){
		if(StringUtils.isEmpty(str)){
			throw new RuntimeException("load crawler file error,file is null!");
		}
		Document document = null;
		try {
            document = this.loadDocFromPath(str);
		} catch (DocumentException e) {
			try {
                document = this.loadDocFromContent(str);
			} catch (DocumentException e1) {
				throw new RuntimeException("load crawler file exception"+str,e1);
			}
		}
		this.load(document);
	}
	
	protected Document loadDocFromPath(String path) throws DocumentException{
		String file = ApplicationResourceUtils.getResourceUrl(path);
		SAXReader reader = new SAXReader();
		Document document = reader.read(file);
		return document;
	}
	
	protected Document loadDocFromContent(String str) throws DocumentException{
		 return DocumentHelper.parseText(str);  
	}
	
}