package com.sdata.core.parser.html.util;

import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;

import org.apache.commons.beanutils.BeanUtils;
import org.dom4j.Element;
import org.dom4j.tree.DefaultAttribute;

import com.sdata.core.parser.html.field.DatumField;
import com.sdata.core.parser.html.field.DatumItem;
import com.sdata.core.parser.html.field.DatumList;
import com.sdata.core.parser.html.field.Field;
import com.sdata.core.parser.html.field.StrategyDatum;
import com.sdata.core.parser.html.field.StrategyInit;
import com.sdata.core.parser.html.field.StrategyLinks;
import com.sdata.core.parser.html.field.Tags;

/**
 * @author zhufb
 * 
 */
public class XmlFieldUtils {

	public static Field getFieldFromXml(Element e) {
		if (e == null) {
			return null;
		}
		Field result = null;
		String name = e.getName();
		if (Tags.FIELD.getName().equals(name)) {
			result = new DatumField();
		} else if (Tags.LIST.getName().equals(name)) {
			result = new DatumList();
		} else if (Tags.ITEM.getName().equals(name)) {
			result = new DatumItem();
		} else if (Tags.INIT.getName().equals(name)) {
			result = new StrategyInit();
		} else if (Tags.LINKS.getName().equals(name)) {
			result = new StrategyLinks();
		} else if (Tags.DATUM.getName().equals(name)) {
			result = new StrategyDatum();
		}else{
			throw new RuntimeException("wrong tag name:"+name);
		}
		// attributes
		copyXmlAttributeToField(result, e);
		// sub elements
		copyXmlChildsToField(result, e);
		return result;
	}

	public static void copyXmlAttributeToField(Field field, Element e) {
		Iterator iterator = e.attributeIterator();
		while (iterator.hasNext()) {
			DefaultAttribute next = (DefaultAttribute) iterator.next();
			String name = next.getName();
			String value = next.getValue();
			try {
				BeanUtils.copyProperty(field, name, value);
			} catch (IllegalAccessException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			} catch (InvocationTargetException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
		}
	}

	public static void copyXmlChildsToField(Field field, Element e) {
		Iterator<Element> iterator = e.elementIterator();
		while (iterator.hasNext()) {
			Element next = iterator.next();
			field.addChildField(getFieldFromXml(next));
		}
	}
}