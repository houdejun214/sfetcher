package com.sdata.core.parser;

import java.math.BigDecimal;
import java.util.Date;

import org.dom4j.DocumentException;
import org.dom4j.DocumentHelper;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.lakeside.core.utils.StringUtils;
import com.lakeside.core.utils.time.DateTimeUtils;
import com.sdata.context.config.SdataConfigurable;
import com.sdata.context.state.RunState;
import com.sdata.core.RawContent;

/**
 * the basic parser object for parsing page content
 * 
 * @author houdejun
 *
 */
public abstract class SdataParser extends SdataConfigurable {

	protected RunState state;
	
	protected void setRunState(RunState state){
		this.state = state;
	}
	
	protected Element getParent(Element el,String tagName){
		Element parent = el.parent();
		while(parent!=null && !parent.tagName().equals(tagName)){
			parent = parent.parent();
		}
		return parent;
	}
	
	protected String selectText(Element doc, String selector) {
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
	
	protected String selectHtml(Element doc, String selector) {
		Element first = doc.select(selector).first();
		if(first==null){
			return "";
		}
		String html = first.html();
		if(html==null){
			html="";
		}
		return html;
	}
	
	protected String selectSiblingBlockContentText(Element doc, String selector,String subSelector){
		Element el = doc.select(selector).first();
		if(el!=null){
			Elements siblingElements = el.siblingElements();
			Element first = siblingElements.select(subSelector).first();
			if(first==null){
				return "";
			}
			String text = first.text();
			if(text==null){
				text="";
			}
			return text;
		}
		return "";
	}
	
	protected String selectSiblingBlockContentHtml(Element doc, String selector,String subSelector){
		Element el = doc.select(selector).first();
		if(el!=null){
			Elements siblingElements = el.siblingElements();
			Element first = siblingElements.select(subSelector).first();
			if(first==null){
				return "";
			}
			String html = first.html();
			if(html==null){
				html="";
			}
			return html;
		}
		return "";
	}
	
	protected Elements selectSiblingBlock(Element doc, String selector,String subSelector){
		Element el = doc.select(selector).first();
		if(el!=null){
			Elements siblingElements = el.siblingElements();
			return siblingElements.select(subSelector);
		}
		return null;
	}

	protected String selectLink(Element doc, String selector) {
		Element first = doc.select(selector).first();
		if(first==null){
			return "";
		}
		String link="";
		if("img".equals(first.tagName())){
			link = first.absUrl("src");
			
		}else if("a".equals(first.tagName())){
			link = first.absUrl("href");
		}
		return link;
	}
	
	protected Document parseHtmlDocument(RawContent c){
		String content = c.getContent();
		if(StringUtils.isEmpty(content)){
			return null;
		}
		Document doc =Jsoup.parse(content,c.getUrl());
		return doc;
	}
	
	protected Document parseHtmlDocument(String content){
		if(StringUtils.isEmpty(content)){
			return null;
		}
		Document doc =Jsoup.parse(content);
		return doc;
	}
	
	protected Document parseHtmlDocument(String content,String baseUri){
		if(StringUtils.isEmpty(content)){
			return null;
		}
		Document doc =Jsoup.parse(content,baseUri);
		return doc;
	}
	
	protected org.dom4j.Document parseXmlDocument(RawContent c){
		String content = c.getContent();
		 
		try {
			org.dom4j.Document doc = DocumentHelper.parseText(content);
			return doc;
		} catch (DocumentException e) {
			
		}
		return null;
	}
	
	protected long toLong(Object value) {
		if(value==null){
			return 0;
		}
		return Long.valueOf(value.toString());
	}

	protected int toInt(Object value) {
		if(value==null){
			return 0;
		}
		return Integer.valueOf(value.toString());
	}
	
	protected int toInt(Object value,int defaultVal) {
		if(value==null){
			return defaultVal;
		}
		return Integer.valueOf(value.toString());
	}

	protected BigDecimal toBigdecimal(Object value) {
		if(value==null){
			return null;
		}
		return new BigDecimal(value.toString());
	}
	
	protected Date toDate(Object value){
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

	protected String toString(Object value) {
		if(value==null){
			return null;
		}
		return value.toString();
	}

	protected boolean toBoolean(Object value) {
		if(value==null){
			return false;
		}
		return Boolean.valueOf(value.toString());
	}

	public ParseResult parseList(RawContent c){
		return new ParseResult();
	}
	
	public ParseResult parseSingle(RawContent c){
		return new ParseResult();
	}

}
