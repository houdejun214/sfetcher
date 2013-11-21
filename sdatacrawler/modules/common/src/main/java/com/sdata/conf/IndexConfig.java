package com.sdata.conf;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.io.SAXReader;

import com.lakeside.core.utils.ApplicationResourceUtils;
import com.lakeside.core.utils.StringUtils;
import com.sdata.core.data.index.solr.IndexSite;
import com.sdata.core.data.index.solr.IndexSource;

/**
 * @author zhufb
 *
 */
public class IndexConfig {
	
	private static final String path ="conf/index-config.xml";
	private static Map<String,IndexSite> map = new HashMap<String,IndexSite>();
	static {
		try {
			String absolutePath = ApplicationResourceUtils.getResourceUrl(path);
			// load xml config file 
			SAXReader reader = new SAXReader();
			File file = new File(absolutePath);
			Document document = null;
			if(!file.exists()){
				InputStream stream = ApplicationResourceUtils.getResourceStream(path);
				document = reader.read(stream);
			}else{
				document = reader.read(file);
			}
	        Element root = document.getRootElement();
	        Iterator<?> iterator = root.elementIterator();
	        
	        while(iterator.hasNext()){
	        	Element site = (Element)iterator.next();
	        	String sname = site.attributeValue("name");
	        	IndexSite solrSite = new IndexSite();
	        	List<IndexSource> list = new ArrayList<IndexSource>();
	        	Iterator sources = site.elementIterator();
	        	while(sources.hasNext()){
	        		Element source = (Element)sources.next();
		        	String name = source.attributeValue("name");
		        	String url = source.attributeValue("url");
		        	String filter = source.attributeValue("filter");
		        	String value = source.attributeValue("value");
		        	String from = source.attributeValue("from");
		        	String to = source.attributeValue("to");
		        	String num = source.attributeValue("num");
		        	IndexSource ss = new IndexSource();
		        	ss.setName(name);
		        	ss.setUrl(url);
		        	ss.setFilter(filter);
		        	ss.setValue(value);
		        	ss.setFrom(from);
		        	ss.setTo(to);
		        	if(!StringUtils.isEmpty(num)){
		        		ss.setNum(Integer.valueOf(num));
		        	}
		        	list.add(ss);
	        	}
	        	solrSite.setList(list);
	        	map.put(sname, solrSite);
	        }
		} catch (Exception e) {
			;
		}
	}

	public static Map<String, IndexSite> getMap() {
		return map;
	}

	public static IndexSite getSite(String name) {
		return map.get(name);
	}
}
