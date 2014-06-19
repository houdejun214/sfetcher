package com.sdata.core.parser.config;

import com.sdata.context.config.Configuration;
import com.sdata.context.parser.config.AbstractConfig;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.strategy.StrategyField;
import com.sdata.core.parser.html.field.strategy.StrategyInit;
import com.sdata.core.parser.html.util.XmlFieldUtils;
import org.dom4j.Document;
import org.dom4j.Element;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhufb
 *
 */
public class StrategyConfig extends AbstractConfig{
	private List<Field> list;
	private static Object syn = new Object();
	private StrategyInit initField;
	public final static String CONF_XML ="strategyXml";
	private final static Map<Configuration,StrategyConfig> configMap  = new ConcurrentHashMap<Configuration,StrategyConfig>();
	
	public static StrategyConfig getInstance(Configuration conf){
		if(!configMap.containsKey(conf)){
			synchronized (syn) {
				if(!configMap.containsKey(conf)) {
					configMap.put(conf,new StrategyConfig(conf));
				}
			}
		}
		return configMap.get(conf);
	}
	
	private StrategyConfig(Configuration conf){
		super(conf);
	}
	
	@Override
	protected void load(Document document) {
		list = new ArrayList<Field>();
		Element root = document.getRootElement();
		Iterator<Element> iterator = root.elementIterator();
		// add notify
		while(iterator.hasNext()){
			Element next = (Element)iterator.next();
			Field fieldFromXml = XmlFieldUtils.getFieldFromXml(next);
			if(fieldFromXml instanceof StrategyInit){
				initField = (StrategyInit)fieldFromXml;
			}else{
				list.add(fieldFromXml);
			}
		}
		
	}

	public String getNextInit() {
		return initField.getNextInit();
	}

	public List<Field> getTag(String tag) {
		List<Field> result = new ArrayList<Field>();
		if(tag == null){
			return null;
		}
		Iterator<Field> fields = getFields();
		while(fields.hasNext()){
			StrategyField field = (StrategyField)fields.next();
			if(tag.equals(field.getName())){
				result.add(field);
			}
		}
		return result;
	}

	public Iterator<Field> getFields() {
		return list.iterator();
	}

	@Override
	protected String getConfXmlKey() {
		return CONF_XML;
	}
}
