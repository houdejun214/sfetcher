package com.sdata.core.util;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;

/**
 * utils for operate html doc
 * 
 *
 * 
 */
public class JsoupUtils {

	public static String getText(Element doc, String selector) {
		Element first = doc.select(selector).first();
		if(first==null){
			return "";
		}
		String text = first.text();
		if(text==null){
			text="";
		}
		return text;
	}
	
	public static String getListText(Element doc, String selector){
		Elements els = doc.select(selector);
		if(els==null){
			return "";
		}
		String text = els.text();
		if(text==null){
			text="";
		}
		return text;
	}
	
	public static String getAttribute(Element doc, String selector,String attributeKey) {
		Element first = doc.select(selector).first();
		if(first==null){
			return "";
		}
		String text = first.attr(attributeKey);
		if(text==null){
			text="";
		}
		return text;
	}
	
	public static List<String> getListLink(Element doc, String selector) {
		List<String> result = new ArrayList<String>();
		Elements list = doc.select(selector);
		for(Element e:list){
			String link = getLink(e,"a");
			if(!StringUtils.isEmpty(link)){
				result.add(link);
			}
		}
		return result;
	}
	
	
	public static List<Element> getList(Element doc, String selector) {
		Elements list = doc.select(selector);
		if(list==null){
			return null;
		}
		Iterator<Element> iterator = list.listIterator();
		return (List<Element>)IteratorUtils.toList(iterator);
	}
	
	public static List<String> getListAttr(Element doc, String selector,String attr) {
		List<String> result = new ArrayList<String>();
		if(doc == null){
			return null;
		}
		Elements list = doc.select(selector);
		if(list==null){
			return null;
		}
		for(Element e:list){
			if(e.hasAttr(attr))
				result.add(e.attr(attr));
		}
		return result;
	}

	public static String getLink(Element doc, String selector) {
		Element first = doc.select(selector).first();
		if(first==null){
			return "";
		}
		String link="";
		if("img".equals(first.tagName())){
			if("".equals(doc.baseUri())){
				link = first.attr("src");
			}else{
				link = first.absUrl("src");
			}
		}else if("a".equals(first.tagName())){
			if("".equals(doc.baseUri())){
				link = first.attr("href");
			}else{
				link = first.absUrl("href");
			}
		}
		return link;
	}

	public static long toLong(Object value) {
		if(value==null){
			return 0;
		}
		return Long.valueOf(value.toString());
	}

	public static int toInt(Object value) {
		if(value==null){
			return 0;
		}
		return Integer.valueOf(value.toString());
	}

	public static BigDecimal toBigdecimal(Object value) {
		if(value==null){
			return null;
		}
		return new BigDecimal(value.toString());
	}
	
	public static Date toDate(Object value){
		if(value==null){
			return null;
		}
		String str_value = value.toString();
		if(StringUtils.isNum(str_value)){
			long num_value = Long.parseLong(str_value);
			if(str_value.length()<=10){
				num_value*=1000;
			}
			return new Date(num_value);
		}else{
			return DateTimeUtils.parse(str_value, "yyyy-MM-dd hh:mm:ss");
		}
	}

	public static String toString(Object value) {
		if(value==null){
			return null;
		}
		return value.toString();
	}

	public static boolean toBoolean(Object value) {
		if(value==null){
			return false;
		}
		return Boolean.valueOf(value.toString());
	}

	public static String getListItemText(List<Element> list, int index, String selector) {
		if(list.size()<=index){
			return "";
		}
		Element el = list.get(index);
		return getText(el,selector);
	}

	public static String getListItemLink(List<Element> list, int index, String selector) {
		if(list.size()<=index){
			return "";
		}
		Element el = list.get(index);
		return getLink(el,selector);
	}
}
