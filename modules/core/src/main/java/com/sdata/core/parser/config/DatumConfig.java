package com.sdata.core.parser.config;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;

import com.sdata.context.config.Configuration;
import com.sdata.context.parser.config.AbstractConfig;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.datum.DatumField;
import com.sdata.core.parser.html.notify.CrawlNotify;
import com.sdata.core.parser.html.notify.DatumNotify;
import com.sdata.core.parser.html.util.XmlFieldUtils;

/**
 * @author zhufb
 *
 */
public class DatumConfig extends AbstractConfig{
	
	private List<Field> list ;
	private Map<String,Field> fieldMap = new ConcurrentHashMap<String,Field>();
	private DatumFilter datumFilter;
	private CrawlNotify crawlNotify;
	public final static String CONF_XML = "datumXml";
	private final static Map<Configuration,DatumConfig> configMap  = new HashMap<Configuration,DatumConfig>();
	
	public static DatumConfig getInstance(Configuration conf){
		if(!configMap.containsKey(conf)){
			synchronized (configMap) {
				if(!configMap.containsKey(conf)) {
					configMap.put(conf,new DatumConfig(conf));
				}
			}
		}
		return configMap.get(conf);
	}
	
	public DatumConfig(String str){
		super(str);
	}
	
	public DatumConfig(Configuration conf){
		super(conf);
	}

	@Override
	protected void parse(Document document) {
		list = new ArrayList<Field>();
		Element root = document.getRootElement();
		//add filter
		this.addDatumFilter(root);
		
		this.addCrawlNotify(root);
		Iterator<Element> iterator = root.elementIterator();
		while(iterator.hasNext()){
			Element next = (Element)iterator.next();
			list.add(XmlFieldUtils.getFieldFromXml(next));
		}
	}
	
	private void addDatumFilter(Element root){
		String v = root.attributeValue("filter");
		datumFilter = new DatumFilter(v);
	}

	private void addCrawlNotify(Element root){
		String v = root.attributeValue("notify");
		crawlNotify = new DatumNotify(conf,v);
	}
	
	public Field getField(String name) {
		if(StringUtils.isEmpty(name)){
			return null;
		}
		Iterator<Field> fields = getFields();
		while(fields.hasNext()){
			Field field = fields.next();
			Object str = field.getName();
			if(str!=null&&str.toString().equals(name)){
				return field;
			}
		}
		return null;
	}

	public Iterator<Field> getFields() {
		return list.iterator();
	}

	public Map<String,Field> getFieldMap() {
		if(fieldMap.size() == 0){
			Iterator<Field> fields = getFields();
			while(fields.hasNext()){
				this.putMap((DatumField)fields.next());
			}
		}
		return fieldMap;
	}
	
	private void putMap(DatumField field){
		Object str = field.getName();
		if(str!=null&&!StringUtils.isEmpty(str.toString())){
			fieldMap.put(str.toString(), field);
		}
	}

	public DatumFilter getDatumFilter() {
		return datumFilter;
	}

	public CrawlNotify getCrawlNotify() {
		return crawlNotify;
	}

	@Override
	protected String getConfXmlKey() {
		return CONF_XML;
	}
}