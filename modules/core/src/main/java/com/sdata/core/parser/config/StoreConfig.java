package com.sdata.core.parser.config;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang.StringUtils;
import org.dom4j.Document;
import org.dom4j.Element;
import org.dom4j.tree.DefaultAttribute;

import com.sdata.context.config.Configuration;
import com.sdata.context.config.Constants;
import com.sdata.context.parser.config.AbstractConfig;
import com.sdata.db.ColumnFamily;
import com.sdata.db.DaoCollection;

/**
 * @author zhufb
 *
 */
public class StoreConfig extends AbstractConfig{
	private List<DaoCollection> list;
	private static Object syn = new Object(); 
	public final static String CONF_XML ="storeXml";
	private transient DaoCollection mainCollection;
	private final static Map<Configuration,StoreConfig> configMap  = new ConcurrentHashMap<Configuration,StoreConfig>();
	public static StoreConfig getInstance(Configuration conf){
		if(!configMap.containsKey(conf)){
			synchronized (syn) {
				if(!configMap.containsKey(conf)) {
					configMap.put(conf,new StoreConfig(conf));
				}
			}
		}
		return configMap.get(conf);
	}

	@Override
	protected void parse(Document document) {
		list = new ArrayList<DaoCollection>();
		Element root = document.getRootElement();
		Iterator<Element> iterator = root.elementIterator();
		while(iterator.hasNext()){
			Element next = (Element)iterator.next();
			DaoCollection collection = new DaoCollection(conf.get(Constants.SOURCE));
			// attribute
			this.copyAttributeToCollection(collection, next);
			// sub element contains update and remove
			this.addChildren(collection, next);
			list.add(collection);
		}
	}
	
	private StoreConfig(Configuration conf){
		super(conf);
	}
	
	private void copyAttributeToCollection(DaoCollection collection,Element e) {
		Iterator iterator = e.attributeIterator();
		while (iterator.hasNext()) {
			DefaultAttribute next = (DefaultAttribute) iterator.next();
			String name = next.getName();
			String value = next.getValue();
			try {
				BeanUtils.copyProperty(collection, name, value);
			} catch (IllegalAccessException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InvocationTargetException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	private void addChildren(DaoCollection collection,Element e) {
		Iterator<Element> iterator = e.elementIterator();
		while (iterator.hasNext()) {
			Element next = iterator.next();
			String name = next.getName();
			String field = next.getText();
			if("update".equals(name)){
				collection.addUpdate(field);
			}else if("remove".equals(name)){
				collection.addRemove(field);
			}else if("colfly".equals(name)){
				String colflyName = next.attribute("name").getValue();
				if(StringUtils.isEmpty(colflyName)){
					throw new RuntimeException("store config colfly attribute name is null!");
				}
				collection.addColumnFamily(new ColumnFamily(colflyName,field));
			}
		}
	}

	public Iterator<DaoCollection> getCollections() {
		return list.iterator();
	}

	public DaoCollection getMainCollection() {
		if(mainCollection == null){
			Iterator<DaoCollection> iterator = list.iterator();
			while(iterator.hasNext()){
				DaoCollection sc = iterator.next();
				if(StringUtils.isEmpty(sc.getField())){
					mainCollection =  sc;
				}
			}
		}
		return mainCollection;
	}

	@Override
	protected String getConfXmlKey() {
		return CONF_XML;
	}
}